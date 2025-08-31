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
	with Path(path).open('w', encoding='utf-8') as f:
		json.dump(data, f, ensure_ascii=False, indent=2)



def load_cookies_for_domain(
	path: Union[str, Path],
	domain: Optional[str] = None,
) -> Tuple[str, Dict[str, str]]:

	def _clean_domain(d: Optional[str]) -> str:
		return (d or '').lstrip('.')

	p = Path(path)
	with p.open('r', encoding='utf-8') as f:
		cookies = json.load(f)
	if isinstance(cookies, dict) and 'cookies' in cookies:
		cookies = cookies['cookies']
	if not isinstance(cookies, list):
		raise ValueError("Unsupported cookies JSON format: expected list or {'cookies': [...]}")

	chosen = _clean_domain(domain)
	if not chosen:
		for c in cookies:
			if c.get('name') == 'wekings_session':
				chosen = _clean_domain(c.get('domain'))
				if chosen:
					break
		if not chosen:
			domains = [_clean_domain(c.get('domain')) for c in cookies if c.get('domain')]
			if not domains:
				raise ValueError("No domains found in cookies")
			chosen = Counter(domains).most_common(1)[0][0]

	applicable: Dict[str, str] = {}
	for c in cookies:
		cd = _clean_domain(c.get('domain'))
		if not cd:
			continue
		if chosen == cd or chosen.endswith('.' + cd) or cd.endswith('.' + chosen):
			name = c.get('name')
			val = c.get('value')
			if name is not None and val is not None:
				applicable[name] = val

	if not applicable:
		raise ValueError(f"No applicable cookies for domain '{chosen}'")

	return chosen, applicable


def fetch_and_parse(
	cookies_path: Union[str, Path],
	url_path: str,
	parse_fn: Callable[[str], Dict[str, Optional[int]]],
	timeout: int = 20,
) -> Dict[str, Optional[int]]:
	domain, cdict = load_cookies_for_domain(cookies_path)
	base = f"https://{domain}"
	url = base + (url_path if url_path.startswith('/') else '/' + url_path)
	headers = {
		"User-Agent": "Mozilla/5.0",
		"Accept": "text/html,application/xhtml+xml",
		"Accept-Language": "ru-RU,ru;q=0.9",
		"Referer": base + "/",
		"Connection": "close",
	}
	with requests.Session() as s:
		jar = requests.cookies.RequestsCookieJar()
		for k, v in cdict.items():
			jar.set(k, v, domain=domain, path="/")
		s.cookies = jar

		r = s.get(url, headers=headers, timeout=timeout, allow_redirects=True)
		r.raise_for_status()
		if 'login' in r.url or ('Вход' in r.text and 'Пароль' in r.text):
			raise RuntimeError("Неавторизован: проверь куки/сессию")

		parsed = parse_fn(r.text) or {}
		parsed["domain"] = domain
		return parsed


def monastic_block(
	html: str,
	thresholds_sec: list[int],
	window_sec: int,
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
		r'<span[^>]*class="[^"]*event-header[^"]*"[^>]*>\s*Владык[ауы]\s+На[её]мников\s*</span>'
		r'.{0,200}?'  # чуть-чуть «вправо» в пределах шапки карточки
		r'<span[^>]*class="[^"]*text-xs[^"]*"[^>]*>\s*([0-9]{2}:[0-9]{2}\s+[0-9]{2}\.[0-9]{2}\.[0-9]{2})\s*</span>',
		re.IGNORECASE | re.DOTALL
	)

	candidates = []
	for mt in card_re.finditer(html):
		when_str = mt.group(1)
		try:
			dt = datetime.strptime(when_str, "%H:%M %d.%m.%y")
		except ValueError:
			continue
		candidates.append((dt, None, when_str))  # города нет -> None

	if not candidates:
		return {"city": None, "when_str": None, "when_iso": None}

	today = date.today()
	todays = [(dt, city, when_str) for dt, city, when_str in candidates if dt.date() == today]
	if not todays:
		return {"city": None, "when_str": None, "when_iso": None}

	best_dt, best_city, best_when = max(todays, key=lambda x: x[0])
	return {
		"city": best_city,               # None, если город не указан на странице
		"when_str": best_when,           # "11:57 22.08.25"
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
	today = datetime.now().date().isoformat()
	state = _load_state(state_file)

	MSK = timezone(timedelta(hours=3))

	def _eta_msk_label(seconds: int) -> str:
		sec = max(int(seconds), 0)
		now_msk = datetime.now(MSK)
		eta = now_msk + timedelta(seconds=sec)
		return f"(в {eta:%H:%M})" if eta.date() == now_msk.date() else f"(в {eta:%H:%M %d.%m})"

	# МОНАХ
	monk = fetch_and_parse(
		cookies_path=cookies_path,
		url_path="/monastic",
		parse_fn=lambda html: monastic_block(html, thresholds_sec, window_sec),
		timeout=timeout,
	)
	dragon_sec = monk.get("dragon")
	serpent_sec = monk.get("serpent")
	print(dragon_sec, serpent_sec)

	EARLY_SLACK_SEC = 10*60
	LATE_SLACK_SEC  = 13*60

	for beast, sec in (("dragon", dragon_sec), ("serpent", serpent_sec)):
		if sec is None:
			continue
		for thr in thresholds_sec:
			delta = thr - sec
			in_early_window = 0 <= delta <= EARLY_SLACK_SEC
			in_late_window  = -LATE_SLACK_SEC <= delta < 0
			if not (in_early_window or in_late_window):
				continue

			key = f"{beast}_{thr}"
			if state.get(key) == today:
				continue

			city = "Гранд" if beast == "dragon" else "Норлунг"
			who  = "дракона" if beast == "dragon" else "змея"
			msg  = (
				"Храбрые викинги, внимание!\n"
				f"Мудрый монах предрекает нападение <b>{who}</b> на <b>{city}</b>\n"
				f"через {_humanize_time_ru(int(sec))}! {_eta_msk_label(sec)}"
			)
			print(msg)
			resp = tg_send(bot_token, chat_ids, msg, parse_mode="HTML")
			print(resp)
			state[key] = today

	# ВЛАД
	merc = fetch_and_parse(
	cookies_path=cookies_path,
	url_path="/events",
	parse_fn=merc_lord_block,
	timeout=timeout,
)

	if merc.get("when_iso") and state.get("lord") != today:

		lord_dt = datetime.fromisoformat(merc["when_iso"])
		battle_dt = lord_dt + timedelta(hours=1)
		time_label = battle_dt.strftime("%H:%M")
		city = merc.get("city")
		if city:
			msg = (
				"Храбрые викинги, внимание!\n"
				f"К городу <b>{city}</b> приближается Владыка Наемников!\n"
				f"Готовьтесь к бою в <b>{time_label}</b>!"
			)
		else:
			msg = (
				"Храбрые викинги, внимание!\n"
				"Приближается Владыка Наемников!\n"
				f"Готовьтесь к бою в <b>{time_label}</b>!"
			)
		print(msg)
		resp = tg_send(bot_token, chat_ids, msg, parse_mode="HTML")
		print(resp)
		state["lord"] = today

	_save_state(state_file, state)


BOT_TOKEN = env_get("BOT_TOKEN", "")
CHAT_IDS = [x.strip() for x in env_get("CHAT_IDS", "").split(",") if x.strip()]
#~ print(CHAT_IDS)
#~ print(requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates").json())

notify_if_needed(
	cookies_path=env_get("COOKIES_FILE", "herald_playwekings.ru.json"),
	bot_token=BOT_TOKEN,
	chat_ids=CHAT_IDS,
	thresholds_sec=[90*60, 40*60, 10*60],
	window_sec=5*60,
	state_file=env_get("STATE_FILE", "notify_state.json"),
)
