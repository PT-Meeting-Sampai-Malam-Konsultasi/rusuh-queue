#!lua name=rusuh_q
--
-- Valkey function library for the queue service (Solution Architecture: "Scripts").
-- Loaded at start with FUNCTION LOAD REPLACE. REPLACE swaps the whole library, so
-- while a rolling deploy runs old and new tasks side by side, keep the previous
-- version of a function (rusuh_x_v1) next to its successor (rusuh_x_v2) until no
-- old task is left. Every key a function touches is passed in KEYS.

-- rusuh_ratelimit_v1: fixed-window counter.
-- KEYS[1] = counter key; ARGV[1] = window in milliseconds.
-- Returns {count in this window, milliseconds until the window resets}.
local function ratelimit_v1(keys, args)
  local window = tonumber(args[1])
  local n = server.call('INCR', keys[1])
  if n == 1 then
    server.call('PEXPIRE', keys[1], window)
    return {n, window}
  end
  local ttl = server.call('PTTL', keys[1])
  if ttl < 0 then
    -- a counter without expiry would block forever; repair it
    server.call('PEXPIRE', keys[1], window)
    ttl = window
  end
  return {n, ttl}
end

server.register_function('rusuh_ratelimit_v1', ratelimit_v1)

-- Valkey's clock in ms. Every lease end and history entry uses it, never a
-- task's clock (FR-19), so tasks with skewed clocks agree.
local function now_ms()
  local t = server.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function int(v) return tonumber(v) or 0 end
local function fmt(n) return string.format('%.0f', n) end

-- Give a key created after bootstrap the event's expiry (cfg.expireAt, ms).
local function ensure_expiry(key, cfg)
  if server.call('PTTL', key) == -1 then
    local at = server.call('HGET', cfg, 'expireAt')
    if at then server.call('PEXPIREAT', key, at) end
  end
end

-- turn tells where a reached place stands (FR-08). hist holds one entry
-- "<tsMs>:<served>" per advancing tick (see rusuh_tick_v1).
-- Returns the turn's end in ms (0 when not reached or unknown) and whether it
-- was MISSED: reached more than window ms ago without a claim. A seq below the
-- trimmed history (cfg.histFloor) was reached before the history window, so it
-- is missed too (the window must not exceed the history window).
local function turn(hist, cfg, seq, served, now, window)
  if seq > served then return 0, false end
  if seq <= int(server.call('HGET', cfg, 'histFloor')) then return 0, true end
  local first = server.call('ZRANGEBYSCORE', hist, seq, '+inf', 'LIMIT', 0, 1)
  if not first[1] then return 0, false end
  local ends = tonumber(string.match(first[1], '^(%d+):')) + window
  return ends, ends < now
end

-- A place value in devs that still waits or may claim: a number that is not missed.
local function live_place(v, hist, cfg, served, now, window)
  local seq = tonumber(v)
  if not seq then return false end
  local _, missed = turn(hist, cfg, seq, served, now, window)
  return not missed
end

-- rusuh_bootstrap_v1: create or refresh one event's queue from core's sale and
-- stock snapshots (D-1, D-2). Idempotent; any task may run it.
-- KEYS: cfg, seq, served, unreserved, resv, frontier
-- ARGV: nowMs, opensAtMs, endsAtMs, expireAtMs, cancelled (0|1), available,
--       maxSlots, rateCap, streamFrom, initialFrontierJSON
-- Returns {"CREATED"|"UPDATED", state}.
local function bootstrap_v1(keys, args)
  local cfg, seq, served, unreserved, resv, frontier = keys[1], keys[2], keys[3], keys[4], keys[5], keys[6]
  local now, opens, expire_at = tonumber(args[1]), tonumber(args[2]), args[4]
  local cancelled = args[5] == '1'
  local status = 'UPDATED'
  local state = server.call('HGET', cfg, 'state')

  if not state then
    status = 'CREATED'
    if cancelled then
      state = 'closed'
      server.call('HSET', cfg, 'reason', 'cancelled')
    elseif tonumber(args[6]) == 0 then
      state = 'closed'
      server.call('HSET', cfg, 'reason', 'sold_out')
    elseif now < opens then state = 'preopen' else state = 'open' end
    server.call('HSET', cfg, 'state', state)
    server.call('SET', seq, 0, 'NX')
    server.call('SET', served, 0, 'NX')
    server.call('SET', unreserved, args[6], 'NX')
    server.call('SET', resv, 0, 'NX')
    server.call('SET', frontier, args[10], 'NX')
  elseif cancelled then
    state = 'closed'
    server.call('HSET', cfg, 'state', state, 'reason', 'cancelled')
  end

  server.call('HSET', cfg, 'opensAt', args[2], 'endsAt', args[3], 'expireAt', expire_at,
    'maxSlots', args[7], 'rateCap', args[8], 'streamFrom', args[9])
  for _, k in ipairs({cfg, seq, served, unreserved, resv, frontier}) do
    server.call('PEXPIREAT', k, expire_at)
  end
  return {status, state}
end

-- rusuh_join_v1: one place per device and per account in one event (ADR-0003, BR-07).
-- KEYS: cfg, seq, devs, pre, accts, served, hist
-- ARGV: device, user ('' for a guest), miss window ms
-- Returns {status, seq, attached (0|1), endsAtMs, served, opensAtMs, turnEndsMs, nowMs}; status is
--   OK (seq set) | PRE (waiting for the draw, seq 0) | CLOSED | NO_EVENT
--   | ACCOUNT_ELSEWHERE (the account holds a live place on another device)
--   | DEVICE_OTHER_ACCOUNT (this place belongs to a different account)
-- accts also maps "d:<device>" -> user, so a guest call can report accountAttached.
-- A place that was used ("x:<seq>") or whose turn was missed is over: the
-- device joins again at the back (FR-18, "Antre lagi").
local function join_v1(keys, args)
  local cfg, seqk, devs, pre, accts, servedk, hist = keys[1], keys[2], keys[3], keys[4], keys[5], keys[6], keys[7]
  local device, user, window = args[1], args[2], int(args[3])

  local c = server.call('HMGET', cfg, 'state', 'endsAt', 'opensAt')
  local state = c[1]
  if not state then return {'NO_EVENT', 0, 0, 0, 0, 0, 0, 0} end
  local ends, opens = int(c[2]), int(c[3])
  local now, served = now_ms(), int(server.call('GET', servedk))
  local function reply(status, seq, attached, turn_ends)
    return {status, seq, attached, ends, served, opens, turn_ends, now}
  end
  if state == 'closed' then return reply('CLOSED', 0, 0, 0) end

  local dev_user = server.call('HGET', accts, 'd:' .. device)
  if user ~= '' then
    if dev_user and dev_user ~= user then return reply('DEVICE_OTHER_ACCOUNT', 0, 0, 0) end
    local owner = server.call('HGET', accts, user)
    if owner and owner ~= device then
      local other = server.call('HGET', devs, owner)
      if other == 'pre' or (other and live_place(other, hist, cfg, served, now, window)) then
        return reply('ACCOUNT_ELSEWHERE', 0, 0, 0)
      end
    end
  end

  local cur = server.call('HGET', devs, device)
  local status, seq = 'OK', 0
  if cur == 'pre' then
    status = 'PRE'
  elseif cur and live_place(cur, hist, cfg, served, now, window) then
    seq = tonumber(cur)
  elseif state == 'preopen' then
    server.call('SADD', pre, device)
    server.call('HSET', devs, device, 'pre')
    ensure_expiry(pre, cfg)
    status = 'PRE'
  else
    -- new place, or the old one was used, expired, missed or left: back of the queue
    seq = server.call('INCR', seqk)
    server.call('HSET', devs, device, seq)
  end
  ensure_expiry(devs, cfg)

  local attached = 0
  if user ~= '' then
    server.call('HSET', accts, user, device, 'd:' .. device, user)
    ensure_expiry(accts, cfg)
    attached = 1
  elseif dev_user then
    attached = 1
  end
  local turn_ends = 0
  if seq > 0 then turn_ends = turn(hist, cfg, seq, served, now, window) end
  return reply(status, seq, attached, turn_ends)
end

-- rusuh_place_v1: read one device's place (getMyPlace). Read-only.
-- KEYS: cfg, devs, accts, served, hist; ARGV: device, miss window ms
-- Returns {state ('' = no event), place value, attached, endsAtMs, served,
--          opensAtMs, turnEndsMs, missed (0|1), nowMs}.
local function place_v1(keys, args)
  local cfg, devs, accts, servedk, hist = keys[1], keys[2], keys[3], keys[4], keys[5]
  local device, window = args[1], int(args[2])
  local c = server.call('HMGET', cfg, 'state', 'endsAt', 'opensAt')
  local now = now_ms()
  if not c[1] then return {'', '', 0, 0, 0, 0, 0, 0, now} end
  local served = int(server.call('GET', servedk))
  local v = server.call('HGET', devs, device) or ''
  local attached = server.call('HEXISTS', accts, 'd:' .. device)
  local turn_ends, missed = 0, false
  if tonumber(v) then turn_ends, missed = turn(hist, cfg, tonumber(v), served, now, window) end
  return {c[1], v, attached, int(c[2]), served, int(c[3]), turn_ends, missed and 1 or 0, now}
end

-- rusuh_leave_v1: end the device's place ("Keluar dari antrian"). Idempotent.
-- KEYS: cfg, devs, pre; ARGV: device. Returns "OK" | "NO_EVENT".
-- The account link stays on the device, but the place is over, so the account
-- may queue on another device (join treats "x:" as ended).
local function leave_v1(keys, args)
  local cfg, devs, pre = keys[1], keys[2], keys[3]
  if server.call('EXISTS', cfg) == 0 then return 'NO_EVENT' end
  local cur = server.call('HGET', devs, args[1])
  if cur == 'pre' then
    server.call('SREM', pre, args[1])
    server.call('HDEL', devs, args[1])
  elseif tonumber(cur) then
    server.call('HSET', devs, args[1], 'x:' .. cur)
  end
  return 'OK'
end

server.register_function('rusuh_bootstrap_v1', bootstrap_v1)
server.register_function('rusuh_join_v1', join_v1)
server.register_function({function_name = 'rusuh_place_v1', callback = place_v1, flags = {'no-writes'}})
server.register_function('rusuh_leave_v1', leave_v1)


-- A lease is "device|seq|units|status|endMs"; status is reserved | held | purged.
local function parse_lease(v)
  local d, s, u, st, e = string.match(v, '^([^|]*)|(%d+)|(%d+)|(%a+)|(%d+)$')
  if not d then return nil end
  return {device = d, seq = tonumber(s), units = tonumber(u), status = st, ends = tonumber(e)}
end

local function lease_value(l)
  return table.concat({l.device, fmt(l.seq), fmt(l.units), l.status, fmt(l.ends)}, '|')
end

-- hist and inv hold one member "<tsMs>:<served>" per advancing tick, scored by
-- served. served only grows, so score order is also time order: popping from
-- the low end drops the oldest entries. The served value of the last dropped
-- entry is kept in cfg[floor_field]: it is served as of the window start.
-- keep_newest leaves the newest entry so the time a seq was reached stays known.
local function trim(key, cutoff, cfg, floor_field, keep_newest)
  local floor = int(server.call('HGET', cfg, floor_field))
  local keep = keep_newest and 1 or 0
  while server.call('ZCARD', key) > keep do
    local first = server.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local ts = tonumber(string.match(first[1], '^(%d+):'))
    if ts >= cutoff then break end
    server.call('ZREM', key, first[1])
    floor = tonumber(first[2])
    server.call('HSET', cfg, floor_field, fmt(floor))
  end
  return floor
end

-- rusuh_lead_v1: take or renew one event's leadership (Admission controller, step 1).
-- KEYS: leader, fence, cfg; ARGV: task id, lease ms
-- Returns {1 = leading | 0, fence, 1 = newly acquired}. A new lease bumps the
-- fence, so every script of the previous leader is refused from then on.
local function lead_v1(keys, args)
  local leader, fence, cfg = keys[1], keys[2], keys[3]
  if server.call('EXISTS', cfg) == 0 then return {0, 0, 0} end
  local cur = server.call('GET', leader)
  if cur == args[1] then
    server.call('PEXPIRE', leader, args[2])
    return {1, int(server.call('GET', fence)), 0}
  end
  if cur then return {0, 0, 0} end
  server.call('SET', leader, args[1], 'PX', args[2])
  local f = server.call('INCR', fence)
  ensure_expiry(fence, cfg)
  return {1, f, 1}
end

-- rusuh_resign_v1: drop the leadership on shutdown so another task takes over
-- within a tick. KEYS: leader; ARGV: task id. Returns 1 when it was ours.
local function resign_v1(keys, args)
  if server.call('GET', keys[1]) == args[1] then
    server.call('DEL', keys[1])
    return 1
  end
  return 0
end

-- rusuh_purge_v1: give back the units of leases whose order window ended more
-- than the grace ago and that core never held (FR-18, BR-04). The lease stays
-- as "purged", so a late booking.held can take its units back (M4).
-- KEYS: cfg, unreserved, resv, active, leases, fence
-- ARGV: fence, graceMs, limit
-- Returns {"OK", purged, stuck held leases} or {"STALE"}.
local function purge_v1(keys, args)
  local unres, resv, active, leases, fence = keys[2], keys[3], keys[4], keys[5], keys[6]
  if server.call('GET', fence) ~= args[1] then return {'STALE', 0, 0} end
  local cutoff = now_ms() - int(args[2])
  local slots = server.call('ZRANGEBYSCORE', active, '-inf', cutoff, 'LIMIT', 0, int(args[3]))
  local purged, stuck = 0, 0
  for _, slot in ipairs(slots) do
    local raw = server.call('HGET', leases, slot)
    local l = raw and parse_lease(raw)
    if not l then
      server.call('ZREM', active, slot)
    elseif l.status == 'held' then
      -- core owns a held lease until booking.paid or booking.expired (M4)
      stuck = stuck + 1
    else
      if l.status == 'reserved' then
        server.call('INCRBY', unres, l.units)
        if int(server.call('DECRBY', resv, l.units)) < 0 then server.call('SET', resv, 0, 'KEEPTTL') end
        purged = purged + 1
      end
      l.status = 'purged'
      server.call('HSET', leases, slot, lease_value(l))
      server.call('ZREM', active, slot)
      if server.call('HGET', leases, 'd:' .. l.device) == slot then
        server.call('HDEL', leases, 'd:' .. l.device)
      end
    end
  end
  return {'OK', purged, stuck}
end

-- speed gives the frontier's ratePerMin, rateLow, rateHigh and rateSource (FR-09,
-- architecture 08 "Waiting time estimate"). measured is ratePerMin over the window.
--
-- Warm-up: at opening every slot fills at once and almost nobody finishes before
-- one average order time (prior_ms), so the measured speed is far too high. With
-- prior_ms > 0 the speed is the guess maxSlots / prior for one order time after
-- the first admission (started, ms), then blends into the measured speed over a
-- minute; rateSource is "prior" until then. prior_ms = 0 turns the guess off.
--
-- Range: the window is cut into 10 slices; rateLow and rateHigh are the 25th and
-- 75th percentile of the slice speeds, clamped to 0.6x..1x and 1x..1.5x of the
-- speed. Slices that start before the warm-up ended are left out. With fewer
-- than 4 usable slices, or on the guess, the range is the full 0.6x..1.5x band.
-- A paused stretch counts as slow slices, as it does in ratePerMin; clients hide
-- the estimate while paused, so it only widens the range after a pause.
local function speed(hist, hist_floor, now, window, started, prior_ms, max_slots, measured, state)
  local e, source = measured, 'measured'
  if prior_ms > 0 and max_slots > 0 and state ~= 'closed' then
    local prior = max_slots * 60000 / prior_ms
    local since = 0
    if started > 0 then since = now - started end
    local w = math.min(1, math.max(0, (since - prior_ms) / 60000))
    e = w * measured + (1 - w) * prior
    if w < 1 then source = 'prior' end
  end
  if math.floor(e) <= 0 then return {rate = 0, low = 0, high = 0, source = source} end

  local slices = {}
  if source == 'measured' and started > 0 then
    local slice = window / 10
    local from = math.max(now - window, started + prior_ms)
    -- served at each slice edge: the last history entry at or before it
    local entries = server.call('ZRANGE', hist, 0, -1)
    local edges, at, i = {}, hist_floor, 1
    for b = 0, 10 do
      local edge = now - window + b * slice
      while entries[i] and tonumber(string.match(entries[i], '^(%d+):')) <= edge do
        at = tonumber(string.match(entries[i], ':(%d+)$'))
        i = i + 1
      end
      edges[b] = at
    end
    for b = 0, 9 do
      if now - window + b * slice >= from then
        slices[#slices + 1] = (edges[b + 1] - edges[b]) * 60000 / slice
      end
    end
  end

  local low, high = 0.6 * e, 1.5 * e
  if #slices >= 4 then
    table.sort(slices)
    local q = function(f) return slices[math.floor(f * (#slices - 1)) + 1] end
    low = math.min(e, math.max(0.6 * e, q(0.25)))
    high = math.max(e, math.min(1.5 * e, q(0.75)))
  end
  return {rate = math.floor(e), low = math.max(1, math.floor(low)), high = math.ceil(high), source = source}
end

-- rusuh_tick_v1: one controller tick for one event (FR-05, FR-07, FR-09, D-7).
-- KEYS: cfg, seq, served, unreserved, resv, held, active, hist, inv, claimed, pre, frontier, fence
-- ARGV: fence, event id, tick channel, history window ms, invitation window ms,
--       prior order time ms (optional; absent or 0 = no warm-up guess, see speed).
--       Optional so tasks on the previous build, which send 5 arguments, keep working.
-- Returns {"OK", frontier JSON, advance, state, seq, served, invitedPending}
--      or {"STALE"} / {"NO_EVENT"}.
--
-- advance = min(free slots, rateCap, stock room, waiting), where
--   free slots = maxSlots - active leases - invitedPending (D-7: invited in the
--     last invitation window and not claimed yet count as provisional slots),
--   stock room = floor((unreserved - 3 * invitedPending) / 3), but 1 while at
--     least one unit is left beyond the pending invitations (near sell-out).
local function tick_v1(keys, args)
  local cfg, seqk, servedk, unresk, resvk, heldk = keys[1], keys[2], keys[3], keys[4], keys[5], keys[6]
  local active, hist, inv, claimed, pre, frontier, fence = keys[7], keys[8], keys[9], keys[10], keys[11], keys[12], keys[13]
  if server.call('GET', fence) ~= args[1] then return {'STALE'} end
  local c = server.call('HMGET', cfg, 'state', 'opensAt', 'endsAt', 'maxSlots', 'rateCap', 'streamFrom', 'hold', 'histStart', 'reason')
  local state = c[1]
  if not state then return {'NO_EVENT'} end
  local now = now_ms()
  local hist_window, inv_window = int(args[4]), int(args[5])

  local seq, served = int(server.call('GET', seqk)), int(server.call('GET', servedk))
  local unres, resv, held = int(server.call('GET', unresk)), int(server.call('GET', resvk)), int(server.call('GET', heldk))

  local inv_floor = trim(inv, now - inv_window, cfg, 'invFloor', false)
  local hist_floor = trim(hist, now - hist_window, cfg, 'histFloor', true)
  server.call('ZREMRANGEBYSCORE', claimed, '-inf', inv_floor)
  local pending = math.max(0, (served - inv_floor) - server.call('ZCARD', claimed))
  local active_n = server.call('ZCARD', active)

  local next_state, reason = state, c[9]
  if state ~= 'closed' then
    reason = nil
    if now >= int(c[3]) then
      next_state, reason = 'closed', 'ended'
    elseif state == 'preopen' then
      -- the draw (M5) empties pre at opening; until then the queue waits
      if now >= int(c[2]) and server.call('SCARD', pre) == 0 then next_state = 'open' end
    elseif unres + resv + held == 0 then
      next_state, reason = 'closed', 'sold_out'
    elseif c[7] == '1' then
      next_state, reason = 'paused', 'operator'
    elseif unres == 0 then
      next_state, reason = 'paused', 'stock'
    else
      next_state = 'open'
    end
  end
  if next_state ~= state then
    server.call('HSET', cfg, 'state', next_state)
    if next_state == 'closed' then server.call('HSET', cfg, 'reason', reason) end
  end

  local advance = 0
  if next_state == 'open' then
    local free = int(c[4]) - active_n - pending
    local room = unres - 3 * pending
    local by_stock = 0
    if room >= 3 then by_stock = math.floor(room / 3) elseif room >= 1 then by_stock = 1 end
    advance = math.max(0, math.min(free, int(c[5]), by_stock, seq - served))
  end
  if advance > 0 then
    served = served + advance
    server.call('SET', servedk, served, 'KEEPTTL')
    local member = fmt(now) .. ':' .. fmt(served)
    server.call('ZADD', hist, served, member)
    server.call('ZADD', inv, served, member)
    ensure_expiry(hist, cfg)
    ensure_expiry(inv, cfg)
    if not c[8] then server.call('HSET', cfg, 'histStart', fmt(now)) end
    pending = pending + advance
  end

  -- ratePerMin: served now minus served at the window start, per minute of the
  -- window (or of the time since the first admission, early in the sale)
  local rate = 0
  local newest = server.call('ZRANGE', hist, -1, -1)
  if newest[1] and tonumber(string.match(newest[1], '^(%d+):')) >= now - hist_window then
    local elapsed = math.min(hist_window, now - int(c[8] or now))
    if elapsed >= 1000 then rate = math.floor((served - hist_floor) * 60000 / elapsed) end
  end
  local r = speed(hist, hist_floor, now, hist_window, int(c[8]), int(args[6]), int(c[4]), rate, next_state)

  local f = cjson.encode({
    event = args[2], state = next_state, served = served,
    sellable = unres, reserved = resv, held = held,
    ratePerMin = r.rate, rateLow = r.low, rateHigh = r.high, rateSource = r.source,
    streamFrom = int(c[6]), reason = reason,
    opensAt = math.floor(int(c[2]) / 1000), ts = math.floor(now / 1000),
  })
  server.call('SET', frontier, f, 'KEEPTTL')
  ensure_expiry(frontier, cfg)
  server.call('SPUBLISH', args[3], f)
  return {'OK', f, advance, next_state, seq, served, pending}
end

-- rusuh_claim_v1: turn a reached place into a lease of up to 3 units (FR-08,
-- FR-17, ADR-0004). Checks in order: closed; the device's place matches the
-- token seq (a live lease of this place is returned again, so a retried claim
-- is idempotent; else EXPIRED); seq > served -> NOT_YET; seq reached more than
-- the miss window ago -> MISSED; slots full -> FULL; no units -> NO_STOCK.
-- KEYS: cfg, served, devs, unreserved, resv, active, leases, hist, claimed, accts
-- ARGV: device, token seq, new slot id, order window ms, miss window ms
-- Returns {status, slot, units, endMs, attached (0|1), nowMs}.
local function claim_v1(keys, args)
  local cfg, servedk, devs, unresk, resvk = keys[1], keys[2], keys[3], keys[4], keys[5]
  local active, leases, hist, claimed, accts = keys[6], keys[7], keys[8], keys[9], keys[10]
  local device, tseq, slot = args[1], args[2], args[3]
  local state = server.call('HGET', cfg, 'state')
  if not state then return {'NO_EVENT', '', 0, 0, 0, 0} end
  if state == 'closed' then return {'CLOSED', '', 0, 0, 0, 0} end
  local now = now_ms()
  local attached = server.call('HEXISTS', accts, 'd:' .. device)
  local function fail(code) return {code, '', 0, 0, attached, now} end

  local cur = server.call('HGET', devs, device)
  if cur == 'x:' .. tseq then
    local mine = server.call('HGET', leases, 'd:' .. device)
    local raw = mine and server.call('HGET', leases, mine)
    local l = raw and parse_lease(raw)
    if l and l.seq == tonumber(tseq) and l.status ~= 'purged' and l.ends > now then
      return {'OK', mine, l.units, l.ends, attached, now}
    end
    return fail('EXPIRED')
  end
  if cur == 'pre' then return fail('NOT_YET') end
  if cur ~= tseq then return fail('EXPIRED') end

  local seq = tonumber(tseq)
  local served = int(server.call('GET', servedk))
  if seq > served then return fail('NOT_YET') end
  local _, missed = turn(hist, cfg, seq, served, now, int(args[5]))
  if missed then return fail('MISSED') end

  if server.call('ZCARD', active) >= int(server.call('HGET', cfg, 'maxSlots')) then return fail('FULL') end
  local unres = int(server.call('GET', unresk))
  if unres <= 0 then return fail('NO_STOCK') end

  local units = math.min(3, unres)
  local ends = now + int(args[4])
  server.call('DECRBY', unresk, units)
  server.call('INCRBY', resvk, units)
  server.call('ZADD', active, ends, slot)
  server.call('HSET', leases, slot, lease_value({device = device, seq = seq, units = units, status = 'reserved', ends = ends}),
    'd:' .. device, slot)
  server.call('HSET', devs, device, 'x:' .. tseq)
  server.call('ZADD', claimed, seq, slot)
  for _, k in ipairs({active, leases, claimed}) do ensure_expiry(k, cfg) end
  return {'OK', slot, units, ends, attached, now}
end

server.register_function('rusuh_lead_v1', lead_v1)
server.register_function('rusuh_resign_v1', resign_v1)
server.register_function('rusuh_purge_v1', purge_v1)
server.register_function('rusuh_tick_v1', tick_v1)
server.register_function('rusuh_claim_v1', claim_v1)
