from __future__ import annotations
import re, json, time, requests, os, sys

from pathlib import Path
from collections import Counter
from typing import Dict, Optional, Union, Iterable, Callable, Tuple
from datetime import datetime, date, timedelta, timezone


def load_env_file(path: str = ".env") -> dict:
	env = {}
	p = Path(path)
	if not p.exists():
		return env
	for line in p.read_text(encoding="utf-8").splitlines():
		line = line.strip()
		if not line or line.startswith("#") or "=" not in line:
			continue
		k, v = line.split("=", 1)
		env[k.strip()] = v.strip()
	return env

_ENV = load_env_file(".env")

def env_get(key: str, default: str = "") -> str:
	return os.getenv(key, _ENV.get(key, default))


def _to_seconds(text: Optional[str]) -> Optional[int]:
	if not text:
		return None
	t = text.strip()
	parts = [p for p in t.split(':') if p != '']
	try:
		parts = list(map(int, parts))
	except ValueError:
		t_digits = ''.join(ch for ch in t if ch.isdigit())
		return int(t_digits) if t_digits.isdigit() else None
	if len(parts) == 3:
		h, m, s = parts
	elif len(parts) == 2:
		h, m, s = 0, parts[0], parts[1]
	elif len(parts) == 1:
		h, m, s = 0, 0, parts[0]
	else:
		return None
	return h*3600 + m*60 + s

def _load_state(path: Union[str, Path]) -> dict:
	p = Path(path)
	if not p.exists():
		return {}
	try:
		with p.open('r', encoding='utf-8') as f:
			return json.load(f)
	except Exception:
		return {}

def _save_state(path: Union[str, Path], data: dict) -> None:
	p = Path(path)
	tmp = p.with_suffix(p.suffix + ".tmp")
	with tmp.open('w', encoding='utf-8') as f:
		json.dump(data, f, ensure_ascii=False, indent=2)
		f.flush()
		os.fsync(f.fileno())
	tmp.replace(p)


class GameUnavailableError(RuntimeError):
    """The game endpoint cannot be reached or its boss data cannot be read."""


class CharacterUnavailableError(RuntimeError):
    """The game replied with an explicit authentication requirement."""


def _load_cookie_bundles(path: Union[str, Path]) -> list[tuple[str, list[dict]]]:
    """Accept one JSON array or {"cookies": [...]} with multiple domains."""
    with Path(path).open("r", encoding="utf-8") as source:
        cookies = json.load(source)
    if isinstance(cookies, dict) and "cookies" in cookies:
        cookies = cookies["cookies"]
    if not isinstance(cookies, list):
        raise ValueError("Cookies must be a JSON array or an object with a 'cookies' array")

    domains: dict[str, list[dict]] = {}
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        domain = str(cookie.get("domain") or "").strip().lstrip(".").lower()
        if not re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)+", domain):
            continue
        if not isinstance(cookie.get("name"), str) or not isinstance(cookie.get("value"), str):
            continue
        domains.setdefault(domain, []).append(cookie)

    if not domains:
        raise ValueError("No cookies with a valid domain, name and value")
    return list(domains.items())


def load_cookies_for_domain(
    path: Union[str, Path], domain: Optional[str] = None
) -> Tuple[str, Dict[str, str]]:
    bundles = _load_cookie_bundles(path)
    chosen = (domain or "").strip().lstrip(".").lower()
    for host, cookies in bundles:
        if not chosen or chosen == host:
            return host, {c["name"]: c["value"] for c in cookies}
    raise ValueError(f"No cookies for domain '{chosen}'")


def fetch_and_parse(
    cookies_path: Union[str, Path],
    url_path: str,
    parse_fn: Callable[[str], Dict[str, Optional[int]]],
    timeout: int = 20,
    preferred_domain: Optional[str] = None,
) -> Dict[str, Optional[int]]:
    """Try each domain with ONLY its own cookies. Never follow cross-site redirects."""
    from urllib.parse import urljoin, urlparse

    bundles = _load_cookie_bundles(cookies_path)
    if preferred_domain:
        preferred = preferred_domain.lower().lstrip(".")
        bundles.sort(key=lambda pair: pair[0] != preferred)

    failures: list[Exception] = []
    for domain, cookies in bundles:
        base = f"https://{domain}"
        url = urljoin(base + "/", url_path.lstrip("/"))
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Referer": base + "/",
            "Connection": "close",
        }
        try:
            with requests.Session() as session:
                jar = requests.cookies.RequestsCookieJar()
                for cookie in cookies:
                    jar.set(
                        cookie["name"], cookie["value"],
                        domain=domain, path=cookie.get("path") or "/",
                        secure=bool(cookie.get("secure", False)),
                    )
                session.cookies = jar

                for _ in range(5):
                    response = session.get(
                        url, headers=headers, timeout=timeout, allow_redirects=False
                    )
                    if 300 <= response.status_code < 400:
                        target = urljoin(url, response.headers.get("Location", ""))
                        parsed_target = urlparse(target)
                        target_path = parsed_target.path.lower().rstrip("/")
                        if target_path in ("/login", "/start", "/registration"):
                            raise CharacterUnavailableError(f"{domain}: redirected to sign-in")
                        if parsed_target.scheme != "https" or parsed_target.hostname != domain:
                            raise GameUnavailableError(
                                f"{domain}: redirect to another origin; cookies were not forwarded"
                            )
                        if target == url:
                            raise GameUnavailableError(f"{domain}: redirect loop")
                        url = target
                        continue
                    break
                else:
                    raise GameUnavailableError(f"{domain}: too many redirects")

                if response.status_code in (401, 419):
                    raise CharacterUnavailableError(f"{domain}: authentication required")
                if response.status_code >= 400:
                    raise GameUnavailableError(
                        f"{domain}: HTTP {response.status_code} on {url_path}"
                    )

                html = response.text
                if re.search(r"type\s*=\s*['\"]?password", html, re.IGNORECASE) or (
                    "Вход в игру" in html and "Пароль" in html
                ):
                    raise CharacterUnavailableError(f"{domain}: login form returned")

                result = parse_fn(html) or {}
                if url_path.strip("/") == "monastic":
                    if result.get("dragon") is None and result.get("serpent") is None:
                        raise GameUnavailableError(
                            f"{domain}: boss timers not found (page changed or inaccessible)"
                        )
                result["domain"] = domain
                return result
        except (CharacterUnavailableError, GameUnavailableError) as error:
            failures.append(error)
            print(f"DOMAIN ERROR: {type(error).__name__}: {error}", file=sys.stderr)
        except requests.exceptions.RequestException as error:
            failure = GameUnavailableError(f"{domain}: network error ({type(error).__name__})")
            failures.append(failure)
            print(f"DOMAIN ERROR: {failure}: {error}", file=sys.stderr)
        except Exception as error:
            failure = GameUnavailableError(f"{domain}: response processing failed ({type(error).__name__})")
            failures.append(failure)
            print(f"DOMAIN ERROR: {failure}", file=sys.stderr)

    # Explicitly confirmed sign-in failure takes precedence over ambiguous network errors.
    character_failure = next(
        (error for error in failures if isinstance(error, CharacterUnavailableError)), None
    )
    if character_failure is not None:
        raise CharacterUnavailableError("All configured domains failed; login required") from character_failure
    raise GameUnavailableError("No configured game domain returned usable data") from (
        failures[-1] if failures else None
    )


def monastic_block(
	html: str,
	city_dragon: str = "Гранд",
	city_serpent: str = "Норлунг",
) -> dict:
	time_re = re.compile(
		r'Предвижу\s+нападение\s+(?P<beast>[^<\n]+?)\s+через\s*(?P<time>(?:\d{1,2}:)?\d{1,2}:\d{2}|\d+)',
		flags=re.IGNORECASE
	)

	out: Dict[str, Optional[int]] = {'dragon': None, 'serpent': None}
	for m in time_re.finditer(html):
		beast = (m.group('beast') or '').strip().lower()
		secs = _to_seconds(m.group('time'))
		if 'дракон' in beast:
			out['dragon'] = secs
		elif 'зме' in beast:
			out['serpent'] = secs
	return out

def merc_lord_block(html: str) -> dict:
	card_re = re.compile(
		r'<div[^>]*class="[^"]*flex\s+flex-col[^"]*"[^>]*>.*?'
		r'<span[^>]*class="[^"]*event-header[^"]*"[^>]*>\s*(?P<hdr>[^<]*?)\s*</span>.*?'
		r'<span[^>]*class="[^"]*text-xs[^"]*"[^>]*>\s*(?P<when>\d{2}:\d{2}\s+\d{2}\.\d{2}\.\d{2})\s*</span>.*?'
		r'</div>.*?'
		r'<p[^>]*class="[^"]*text-sm[^"]*"[^>]*>\s*(?P<body>.*?)\s*</p>',
		re.IGNORECASE | re.DOTALL
	)

	lord_re = re.compile(r'Владык[а-яё]*\s+На[её]мник[а-яё]*', re.IGNORECASE)
	city_re = re.compile(r'к\s+бою\s+в\s+(?P<city>[А-ЯЁа-яё-]+)', re.IGNORECASE)

	def normalize_city(word: str) -> str | None:
		w = (word or "").strip().rstrip("!?.:,;").lower()
		if w.startswith("гранд"):
			return "Гранд"
		if w.startswith("норлунг"):
			return "Норлунг"
		return None

	candidates = []
	for m in card_re.finditer(html):
		hdr = (m.group("hdr") or "").strip()
		body = (m.group("body") or "").strip()
		if not (lord_re.search(hdr) or lord_re.search(body)):
			continue

		when_str = m.group("when")
		try:
			dt = datetime.strptime(when_str, "%H:%M %d.%m.%y")
		except ValueError:
			continue

		body_text = re.sub(r"<[^>]+>", " ", body)
		body_text = re.sub(r"\s+", " ", body_text).strip()

		mcity = city_re.search(body_text)
		city = normalize_city(mcity.group("city")) if mcity else None

		if city is None:
			continue

		candidates.append((dt, city, when_str))

	if not candidates:
		return {"city": None, "when_str": None, "when_iso": None}

	MSK = timezone(timedelta(hours=3))
	today = datetime.now(MSK).date()
	todays = [(dt, city, when_str) for dt, city, when_str in candidates if dt.date() == today]
	if not todays:
		return {"city": None, "when_str": None, "when_iso": None}

	best_dt, best_city, best_when = max(todays, key=lambda x: x[0])
	return {
		"city": best_city,
		"when_str": best_when,
		"when_iso": best_dt.isoformat(timespec="seconds"),
	}


def _tg_post(method: str, token: str, payload: dict, timeout: int = 20) -> dict:
	url = f"https://api.telegram.org/bot{token}/{method}"
	retries = 3
	for attempt in range(retries):
		try:
			resp = requests.post(url, json=payload, timeout=timeout)
			if resp.status_code == 429:
				retry_after = int(resp.headers.get("Retry-After", "1"))
				time.sleep(retry_after)
				continue
			if resp.status_code >= 500:
				time.sleep(1.5)
				continue
			return resp.json()
		except requests.RequestException as e:
			if attempt == retries - 1:
				return {"ok": False, "error": str(e)}
			time.sleep(1.5)
	return {"ok": False, "error": "Failed after retries"}


def tg_send(
	token: str,
	chat_ids: Union[str, int, Iterable[Union[str, int]]],
	text: str,
	parse_mode: str | None = None,
	disable_notification: bool = False,
	timeout: int = 20,
	sleep_between: float = 0.05,
) -> list[dict]:
	if isinstance(chat_ids, (str, int)):
		chat_ids = [chat_ids]

	results: list[dict] = []
	payload_base = {"text": text, "disable_notification": disable_notification}
	if parse_mode:
		payload_base["parse_mode"] = parse_mode

	for cid in chat_ids:
		payload = {"chat_id": str(cid), **payload_base}
		resp = _tg_post("sendMessage", token, payload, timeout=timeout)
		results.append({"chat_id": cid, **resp})
		time.sleep(sleep_between)
	return results


def _send_notification_once(
    state: dict,
    key: str,
    day: str,
    bot_token: str,
    chat_ids: list[Union[str, int]],
    message: str,
) -> bool:
    """Record successful recipients and retry only undelivered ones."""
    if state.get(key) == day:
        return True
    pending = state.setdefault("_pending_notifications", {})
    record = pending.get(key)
    if not isinstance(record, dict) or record.get("day") != day:
        record = {"day": day, "sent_to": []}
        pending[key] = record
    delivered = set(map(str, record.get("sent_to", [])))
    for response in tg_send(
        bot_token, [cid for cid in chat_ids if str(cid) not in delivered],
        message, parse_mode="HTML"
    ):
        cid = str(response["chat_id"])
        if response.get("ok") is True:
            delivered.add(cid)
        else:
            print(
                f"TELEGRAM ERROR chat={cid}: {response.get('description', 'request failed')}",
                file=sys.stderr
            )
    record["sent_to"] = sorted(delivered)
    if all(str(cid) in delivered for cid in chat_ids):
        state[key] = day
        pending.pop(key, None)
        if not pending:
            state.pop("_pending_notifications", None)
        return True
    return False


def _report_access_failure(
    state: dict, error: Exception, bot_token: str,
    chat_ids: list[Union[str, int]]
) -> None:
    """Only one alert per chat until the game becomes readable again."""
    record = state.get("access_alert")
    if not isinstance(record, dict):
        record = {"sent_to": []}
        state["access_alert"] = record
    delivered = set(map(str, record.get("sent_to", [])))
    recipients = [cid for cid in chat_ids if str(cid) not in delivered]
    if not recipients:
        return

    message = (
        "Нет доступа к персонажу"
        if isinstance(error, CharacterUnavailableError)
        else "Нет доступа к игре"
    )

    try:
        for response in tg_send(bot_token, recipients, message):
            cid = str(response["chat_id"])
            if response.get("ok") is True:
                delivered.add(cid)
            else:
                print(
                    f"TELEGRAM ALERT ERROR chat={cid}: {response.get('description', 'request failed')}",
                    file=sys.stderr
                )
    except Exception as send_error:
        print(f"TELEGRAM ALERT ERROR: {type(send_error).__name__}", file=sys.stderr)
    record["sent_to"] = sorted(delivered)


def _humanize_time_ru(sec: int) -> str:
	if sec < 60:
		return f"{sec} с"
	minutes = sec // 60
	if minutes < 60:
		return f"{minutes} мин"
	hours = minutes // 60
	minutes = minutes % 60
	if minutes == 0:
		return f"{hours} ч"
	return f"{hours} ч {minutes} мин"



def notify_if_needed(
	cookies_path: Union[str, Path],
	bot_token: str,
	chat_ids: list[Union[str, int]],
	thresholds_sec: list[int] = [3600],
	window_sec: int = 300,
	timeout: int = 20,
	state_file: Union[str, Path] = "notify_state.json",
) -> None:
	state = _load_state(state_file)

	MSK = timezone(timedelta(hours=3))
	today = datetime.now(MSK).date().isoformat()

	def _round_to_minute(dt: datetime) -> datetime:
		return dt.replace(second=0, microsecond=0)

	def _eta_label_dt(eta: datetime, now_msk: datetime) -> str:
		return f"(в {eta:%H:%M})" if eta.date() == now_msk.date() else f"(в {eta:%H:%M %d.%m})"

	try:
		# МОНАХ: report the first failure and reset only after a valid read.
		try:
			monk = fetch_and_parse(
				cookies_path=cookies_path,
				url_path="/monastic",
				parse_fn=monastic_block,
				timeout=timeout,
			)
			if monk.get("dragon") is None and monk.get("serpent") is None:
				raise GameUnavailableError("No boss timers found on /monastic")
		except Exception as error:
			_report_access_failure(state, error, bot_token, chat_ids)
			raise
		else:
			if state.pop("access_alert", None) is not None:
				print("Game access restored; outage alert reset")

		delivery_failed = False
		dragon_sec = monk.get("dragon")
		serpent_sec = monk.get("serpent")
		print(dragon_sec, serpent_sec)
		now_msk = datetime.now(MSK)

		thresholds_sorted = sorted(thresholds_sec)

		for beast, sec in (("dragon", dragon_sec), ("serpent", serpent_sec)):
			if sec is None:
				continue
			sec = int(sec)
			if sec <= 0:
				continue

			# 1) вычисляем "предполагаемое" время прилёта и округляем до минуты
			proposed_event_dt = _round_to_minute(now_msk + timedelta(seconds=sec))

			# 2) достаём/фиксируем время прилёта на сегодня
			event_day_key = f"{beast}_event_day"
			event_iso_key = f"{beast}_event_iso"

			event_dt: datetime | None = None
			if state.get(event_day_key) == today and state.get(event_iso_key):
				try:
					event_dt = datetime.fromisoformat(state[event_iso_key])
					if event_dt.tzinfo is None:
						event_dt = event_dt.replace(tzinfo=MSK)
				except Exception:
					event_dt = None

			# если на сегодня не было — фиксируем; если было, но разъехалось сильно — обновляем
			if event_dt is None:
				event_dt = proposed_event_dt
				state[event_day_key] = today
				state[event_iso_key] = event_dt.isoformat(timespec="seconds")
			else:
				if abs(int((event_dt - proposed_event_dt).total_seconds())) >= 120:
					event_dt = proposed_event_dt
					state[event_day_key] = today
					state[event_iso_key] = event_dt.isoformat(timespec="seconds")

			# 3) считаем оставшееся время уже от фиксированного event_dt
			sec_left = int((event_dt - now_msk).total_seconds())
			if sec_left <= 0:
				continue

			due = [thr for thr in thresholds_sorted if sec_left <= thr]
			if not due:
				continue

			thr_to_send = min(due)
			key = f"{beast}_{thr_to_send}"
			if state.get(key) == today:
				continue

			city = "Гранд" if beast == "dragon" else "Норлунг"
			who  = "Дракон" if beast == "dragon" else "Змей"
			dot = "🔴" if beast == "dragon" else "🟢"

			msg = f"{dot} {who} через {_humanize_time_ru(sec_left)}! {_eta_label_dt(event_dt, now_msk)}"
			print(msg)
			if not _send_notification_once(state, key, today, bot_token, chat_ids, msg):
				delivery_failed = True

		# ВЛАД
		merc = fetch_and_parse(
			cookies_path=cookies_path,
			url_path="/events",
			parse_fn=merc_lord_block,
			timeout=timeout,
			preferred_domain=monk["domain"],
		)

		if merc.get("when_iso") and state.get("lord") != today:

			lord_dt = datetime.fromisoformat(merc["when_iso"])

			if lord_dt.tzinfo is None:
				lord_dt = lord_dt.replace(tzinfo=MSK)

			battle_dt = lord_dt + timedelta(hours=1)
			now_msk = datetime.now(MSK)
			sec_left = int((battle_dt - now_msk).total_seconds())

			if sec_left <= 0:
				state["lord"] = today
			else:
				city = merc.get("city")
				loc = "" if not city else (" в Гранде" if city == "Гранд" else (" в Норлунге" if city == "Норлунг" else f" в {city}"))
				msg = f"🔵 Владыка Наемников будет{loc} через {_humanize_time_ru(sec_left)}! {_eta_label_dt(battle_dt, now_msk)}"
				print(msg)
				if not _send_notification_once(state, "lord", today, bot_token, chat_ids, msg):
					delivery_failed = True

		if delivery_failed:
			raise RuntimeError("Telegram rejected one or more notifications")
	finally:
		_save_state(state_file, state)

def _run_once() -> None:
	bot_token = env_get("BOT_TOKEN", "")
	chat_ids = [x.strip() for x in env_get("CHAT_IDS", "").split(",") if x.strip()]
	if not bot_token or not chat_ids:
		raise RuntimeError("BOT_TOKEN or CHAT_IDS missing")

	notify_if_needed(
		cookies_path=env_get("COOKIES_FILE", "herald_playwekings.ru.json"),
		bot_token=bot_token,
		chat_ids=chat_ids,
		thresholds_sec=[91*60, 46*60, 16*60],
		window_sec=5*60,
		state_file=env_get("STATE_FILE", "notify_state.json"),
	)

def main() -> None:
	loop_seconds = int(env_get("LOOP_SECONDS", "0") or "0")
	tick_seconds = int(env_get("TICK_SECONDS", "60") or "60")
	if loop_seconds <= 0:
		_run_once()
		return

	end_ts = time.time() + loop_seconds
	last_failed = False
	while True:
		try:
			_run_once()
			last_failed = False
		except Exception as e:
			last_failed = True
			print(f"ERROR: {e}", file=sys.stderr)
		remaining = end_ts - time.time()
		if remaining <= 0:
			break
		time.sleep(min(tick_seconds, max(1, int(remaining))))
	if last_failed:
		raise RuntimeError("Herald's last poll failed")

if __name__ == "__main__":
	main()
