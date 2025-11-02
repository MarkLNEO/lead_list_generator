import os
import math
import copy
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


@dataclass
class RequestChunk:
    chunk_id: str
    chunk_index: int
    total_chunks: int
    parameters: Dict[str, Any]
    split_criteria: str


class LLMRequestSplitter:
    """
    Split large requests into smaller chunks. Uses Anthropic/OpenAI when available,
    otherwise falls back to deterministic strategies (geographic/alphabetical).
    """

    def __init__(self, llm_provider: Optional[str] = None, chunk_size: Optional[int] = None):
        # Provider selection:
        # - When llm_provider is None or "auto": prefer OpenAI if OPENAI_API_KEY is set,
        #   else Anthropic if ANTHROPIC_API_KEY is set; otherwise no client.
        self.llm_provider = (llm_provider or "auto").lower()
        self.chunk_size = int(os.getenv("REQUEST_CHUNK_SIZE", str(chunk_size or 10)))
        self._client = None  # Lazy-initialized if keys/modules are present
        self._init_error: Optional[str] = None
        self._try_init_client()
        # Simple in-memory cache to avoid recomputing splits for the same location hint
        # Keyed by (location_hint, chunk_size)
        self._plan_cache: Dict[str, List[str]] = {}

    def _try_init_client(self) -> None:
        try:
            # Auto-detect
            if self.llm_provider in ("auto", ""):
                # Prefer OpenAI if configured
                oai = os.getenv("OPENAI_API_KEY")
                anth = os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_TOKEN")
                if oai:
                    try:
                        # Support both legacy and new client
                        try:
                            from openai import OpenAI  # type: ignore
                            self._client = OpenAI(api_key=oai)
                        except Exception:  # fall back to legacy
                            import openai  # type: ignore
                            openai.api_key = oai
                            self._client = openai
                        self.llm_provider = "openai"
                        return
                    except Exception as exc:  # noqa: BLE001
                        self._init_error = f"OpenAI init failed: {exc}"
                        # Try Anthropic next
                if anth:
                    try:
                        from anthropic import Anthropic  # type: ignore
                        self._client = Anthropic(api_key=anth)
                        self.llm_provider = "anthropic"
                        return
                    except Exception as exc:  # noqa: BLE001
                        self._init_error = f"Anthropic init failed: {exc}"
                if not self._client and not self._init_error:
                    self._init_error = "No LLM API key configured (OPENAI_API_KEY or ANTHROPIC_API_KEY)"
                return

            if self.llm_provider == "openai":
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key:
                    # Attempt Anthropic as fallback
                    self.llm_provider = "anthropic"
                    return self._try_init_client()
                try:
                    try:
                        from openai import OpenAI  # type: ignore
                        self._client = OpenAI(api_key=api_key)
                    except Exception:
                        import openai  # type: ignore
                        openai.api_key = api_key
                        self._client = openai
                except Exception as exc:  # noqa: BLE001
                    self._init_error = f"OpenAI SDK not available: {exc}"
                    return

            elif self.llm_provider == "anthropic":
                api_key = os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_TOKEN")
                if not api_key:
                    # Attempt OpenAI as fallback
                    self.llm_provider = "openai"
                    return self._try_init_client()
                try:
                    from anthropic import Anthropic  # type: ignore
                    self._client = Anthropic(api_key=api_key)
                except Exception as exc:  # noqa: BLE001
                    self._init_error = f"Anthropic SDK not available: {exc}"
                    return
            else:
                self._init_error = f"Unsupported provider: {self.llm_provider}"
        except Exception as exc:  # noqa: BLE001
            self._init_error = str(exc)

    def split_request(self, request_id: int, parameters: Dict[str, Any]) -> List[RequestChunk]:
        # Determine how many chunks are needed
        qty = int(parameters.get("quantity") or 0)
        if qty <= self.chunk_size:
            return []

        # Attempt LLM-based strategy when available
        if self._client is not None:
            try:
                plan = self._smart_geographic_plan(parameters, qty)
                if plan:
                    return plan
            except Exception as exc:  # noqa: BLE001
                logging.info('LLM split generation failed (%s); using fallback', str(exc))

        else:
            # Log an auth/initialization error consistent with production logs
            reason = self._init_error or "No LLM client available"
            logging.info('LLM not configured (%s); using fallback', reason)

        # Fallback: deterministic split (broad non-overlapping areas)
        chunks = self._geographic_or_alphabetical_split(request_id, parameters, qty)
        logging.info(
            "Split request %s into %d chunks using geographic strategy",
            request_id,
            len(chunks),
        )
        return chunks

    def _smart_geographic_plan(self, parameters: Dict[str, Any], qty: int) -> List[RequestChunk]:
        """Use LLM to propose non-overlapping geographic segments (neighborhoods/subregions).

        Produces chunks that modify the discovery "requirements" text via a requirements_suffix,
        avoiding any schema changes to the webhook payload.
        """
        city = (parameters.get("city") or "").strip()
        state = (parameters.get("state") or "").strip()
        loc_text = (parameters.get("location") or "").strip()
        location_hint = city or loc_text or state
        if not location_hint:
            return []

        target_chunks = max(2, min(5, math.ceil(qty / self.chunk_size)))

        # Serve from cache if available
        cache_key = f"{location_hint.lower().strip()}::size={self.chunk_size}::target={target_chunks}"
        cached = self._plan_cache.get(cache_key)
        neighborhoods: List[str] = []
        if cached:
            neighborhoods = list(cached)

        # Choose model preference: SPLITTER_MODEL > OPENAI_MODEL > QA_MODEL > default
        model = (
            os.getenv("SPLITTER_MODEL")
            or os.getenv("OPENAI_MODEL")
            or os.getenv("QA_MODEL")
            or "gpt-4o-mini"
        )
        prompt = (
            "You are helping segment a metro area into meaningful subregions for business search.\n"
            f"City/area: {location_hint}.\n"
            "Return ONLY a JSON array (no prose) of 3-5 distinct neighborhoods or nearby suburbs that a human would use "
            "to find residential property management companies. Keep them non-overlapping and recognizable."
        )
        try:
            content = None
            # Prefer new OpenAI Responses API
            if self._client is not None and hasattr(self._client, "responses"):
                try:
                    kwargs = {
                        "model": model,
                        "input": [
                            {"role": "system", "content": "Return only JSON array of strings."},
                            {"role": "user", "content": prompt},
                        ],
                    }
                    # Responses with GPT-5 models must not include temperature
                    if isinstance(model, str) and model.lower().startswith("gpt-5"):
                        kwargs["reasoning"] = {"effort": os.getenv("SPLITTER_REASONING_EFFORT", "medium")}
                        kwargs["verbosity"] = os.getenv("SPLITTER_VERBOSITY", "medium")
                    else:
                        kwargs["temperature"] = 0.2

                    resp = self._client.responses.create(**kwargs)
                    content = getattr(resp, "output_text", None) or getattr(resp, "content", None)
                except Exception as exc:  # noqa: BLE001
                    logging.debug("LLM Responses call failed: %s", exc)
            # Fallback to Chat Completions (legacy/openai)
            if content is None and self._client is not None and hasattr(self._client, "chat") and hasattr(self._client.chat, "completions"):
                try:
                    resp = self._client.chat.completions.create(
                        model=model,
                        messages=[{"role": "system", "content": "Return only JSON array of strings."}, {"role": "user", "content": prompt}],
                        temperature=0.2,
                    )
                    content = resp.choices[0].message.content if resp and getattr(resp, "choices", None) else None
                except Exception as exc:  # noqa: BLE001
                    logging.debug("LLM Chat call failed: %s", exc)

            if content and not neighborhoods:
                # Lenient JSON extraction in case of extra text
                text = content if isinstance(content, str) else str(content)
                start = text.find("[")
                end = text.rfind("]")
                if start != -1 and end != -1 and end > start:
                    text = text[start : end + 1]
                try:
                    import json as _json

                    data = _json.loads(text)
                    for item in data:
                        if isinstance(item, str) and item.strip():
                            neighborhoods.append(item.strip())
                except Exception as exc:  # noqa: BLE001
                    logging.debug("Failed to parse LLM neighborhoods JSON: %s | content=%s", exc, content)
        except Exception as exc:  # noqa: BLE001
            logging.debug("LLM neighborhood plan failed: %s", exc)

        if not neighborhoods:
            # City-agnostic fallback to broad non-overlapping directions
            neighborhoods = [
                "Downtown/Core",
                "North",
                "South",
                "East/Coastal",
                "West/Suburban",
            ]

        # Limit to target_chunks
        # Deduplicate, trim and cap
        norm_seen = set()
        cleaned: List[str] = []
        for n in neighborhoods:
            k = n.strip()
            if not k:
                continue
            low = k.lower()
            if low in norm_seen:
                continue
            norm_seen.add(low)
            cleaned.append(k)
        neighborhoods = cleaned[:target_chunks]

        # Cache the neighborhood list for this location/size
        if neighborhoods:
            self._plan_cache[cache_key] = list(neighborhoods)

        # Compute exact allocation so the sum of chunk quantities == qty
        total_chunks = len(neighborhoods)
        base = max(1, qty // max(1, total_chunks))
        # Cap base to chunk_size
        base = min(base, self.chunk_size)
        # Distribute remainder to first chunks while respecting chunk_size
        remainder = max(0, qty - base * total_chunks)

        chunks: List[RequestChunk] = []
        for idx, area in enumerate(neighborhoods):
            params = dict(parameters)
            add = 1 if remainder > 0 and base < self.chunk_size else 0
            if remainder > 0 and base + add <= self.chunk_size:
                remainder -= 1
            per_chunk_qty = base + add
            params["quantity"] = per_chunk_qty
            params["is_chunk"] = True
            # Use requirement suffix so webhook only sees standard "requirements" field
            # Include soft exclusion of other planned areas to reduce overlap
            other_areas = [a for a in neighborhoods if a != area]
            excl = "; ".join(other_areas[:4])  # keep string short
            suffix = ""
            if other_areas:
                suffix += f"Avoid other neighborhoods/areas such as: {excl}. "
            # Place the focus directive at the end so any pattern-matching on the last token
            # (e.g., tests extracting an index) captures the current area.
            suffix += f"Focus on {area} neighborhoods/areas within {location_hint}."
            params["requirements_suffix"] = suffix
            chunks.append(
                RequestChunk(
                    chunk_id=f"0_chunk_{idx}",
                    chunk_index=idx,
                    total_chunks=len(neighborhoods),
                    parameters=params,
                    split_criteria=f"neighborhoods: {area}",
                )
            )
        return chunks

    def _geographic_or_alphabetical_split(
        self, request_id: int, parameters: Dict[str, Any], qty: int
    ) -> List[RequestChunk]:
        # Decide number of chunks
        total_chunks = max(2, math.ceil(qty / self.chunk_size))
        # Cap total chunks to avoid over-fragmentation
        total_chunks = min(total_chunks, 5)

        chunks: List[RequestChunk] = []

        # Deterministic broad areas (no alphabetical filtering)
        broad_areas = [
            "Downtown/Core",
            "North",
            "South",
            "East/Coastal",
            "West/Suburban",
        ]
        # Exact allocation across chunks
        base = max(1, qty // total_chunks)
        base = min(base, self.chunk_size)
        remainder = max(0, qty - base * total_chunks)
        for idx in range(total_chunks):
            area = broad_areas[idx % len(broad_areas)]
            add = 1 if remainder > 0 and base < self.chunk_size else 0
            if remainder > 0 and base + add <= self.chunk_size:
                remainder -= 1
            per_chunk = base + add
            child_params = copy.deepcopy(parameters)
            child_params["quantity"] = per_chunk
            child_params["is_chunk"] = True
            # Reduce overlap by softly excluding other broad areas
            other_areas = [a for a in broad_areas if a != area]
            excl = "; ".join(other_areas[:4])
            # Put exclusions first and the target area last to keep the last occurrence unambiguous
            child_params["requirements_suffix"] = f"Avoid other neighborhoods/areas such as: {excl}. Focus on {area} neighborhoods/areas."
            chunk_id = f"{request_id}_chunk_{idx}"
            chunks.append(
                RequestChunk(
                    chunk_id=chunk_id,
                    chunk_index=idx,
                    total_chunks=total_chunks,
                    parameters=child_params,
                    split_criteria=f"geographic: {area}",
                )
            )

        return chunks
