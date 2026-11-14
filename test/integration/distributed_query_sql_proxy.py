"""Loopback-only HTTP/2 frame relay; observe real RPC messages or disrupt delivery."""
import os
import socket
import threading
import time


class Proxy:
    def __init__(self, upstream):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', int(os.environ.get('DQ_TEST_PROXY_PORT', '0'))))
        self.listener.listen()
        self.listener.settimeout(.2)
        self.port = self.listener.getsockname()[1]
        self.upstream = upstream
        self.mode = 'pass'
        self.events = []
        self.release = threading.Event()
        self.closed = threading.Event()
        self.sockets = []
        self.threads = []
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.closed.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                server = socket.create_connection(('127.0.0.1', self.upstream), timeout=5)
            except OSError as exc:
                self.events.append({'upstreamConnectError': str(exc)})
                client.close()
                continue
            server.settimeout(None)
            self.sockets.extend([client, server])
            state = {'connection': len(self.sockets)//2, 'target': None,
                     'up': server, 'upLock': threading.Lock(), 'downLock': threading.Lock()}
            for direction, src, dst in [('request', client, server), ('response', server, client)]:
                t = threading.Thread(target=self.relay, args=(src, dst, direction, state), daemon=True)
                self.threads.append(t)
                t.start()

    @staticmethod
    def send(dst, payload, direction, state):
        with state['upLock' if direction == 'request' else 'downLock']:
            dst.sendall(payload)

    def relay(self, src, dst, direction, state):
        buf = bytearray()
        messages = {}
        preface = direction == 'request'
        try:
            while not self.closed.is_set():
                data = src.recv(65536)
                if not data:
                    break
                buf.extend(data)
                if preface:
                    if len(buf) < 24:
                        continue
                    assert bytes(buf[:24]) == b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
                    self.send(dst, buf[:24], direction, state); del buf[:24]; preface = False
                while len(buf) >= 9:
                    size = int.from_bytes(buf[:3], 'big')
                    if len(buf) < 9 + size:
                        break
                    frame = bytes(buf[:9+size]); del buf[:9+size]
                    kind, flags, stream = frame[3], frame[4], int.from_bytes(frame[5:9], 'big') & 0x7fffffff
                    ev = {'direction': direction, 'type': kind, 'stream': stream,
                          'size': size, 'mode': self.mode, 'connection': state['connection']}
                    # Keep a cancellation fixture genuinely in flight at the worker.
                    # This mode must be selected before opening the HTTP/2 connection.
                    if direction == 'request' and self.mode in ('stall-active', 'stall-active-multiplexed'):
                        if kind == 1 and state['target'] is None:
                            state['target'] = stream
                        if kind == 4 and not (flags & 1):
                            settings = bytearray(frame[9:])
                            credit = 65535
                            found = False
                            for offset in range(0, len(settings), 6):
                                if int.from_bytes(settings[offset:offset+2], 'big') == 4:
                                    settings[offset+2:offset+6] = credit.to_bytes(4, 'big')
                                    found = True
                            if not found:
                                settings.extend((4).to_bytes(2, 'big') + credit.to_bytes(4, 'big'))
                            frame = len(settings).to_bytes(3, 'big') + frame[3:9] + bytes(settings)
                            ev['limitedResponseWindow'] = credit
                        if kind == 8 and stream and (self.mode == 'stall-active' or stream == state['target']):
                            ev['suppressedWindowUpdate'] = True
                            self.events.append(ev)
                            continue
                    complete = 0
                    if kind == 0 and stream:
                        payload = frame[9:]
                        if flags & 8:
                            payload = payload[1:len(payload)-payload[0]]
                        pending = messages.setdefault(stream, bytearray()); pending.extend(payload)
                        rows = []
                        while len(pending) >= 5:
                            length = int.from_bytes(pending[1:5], 'big')
                            if len(pending) < 5 + length:
                                break
                            assert pending[0] == 0, 'compressed RPC response not supported by fault relay'
                            raw = bytes(pending[5:5+length]); del pending[:5+length]
                            complete += 1
                            if direction == 'response':
                                rows.append(self.record_count(raw))
                        ev['grpcMessages'] = complete
                        if rows:
                            ev['recordCounts'] = rows
                    if (direction == 'response' and kind == 0 and
                            self.mode == 'stall-active-multiplexed' and stream == state['target']):
                        # Withhold this stream only. Return connection-level credit so that
                        # unrelated streams can progress on the same HTTP/2 connection.
                        ev['held'] = True
                        ev['multiplexedHold'] = True
                        self.events.append(ev)
                        if size:
                            update = (4).to_bytes(3, 'big') + bytes([8, 0]) + (0).to_bytes(4, 'big') + size.to_bytes(4, 'big')
                            self.send(src, update, 'request', state)
                        continue
                    if direction == 'response' and kind == 0 and self.mode in ('stall', 'stall-active'):
                        ev['held'] = True
                        self.events.append(ev)
                        released = self.release.wait(timeout=45)
                        if not released:
                            ev['holdExpired'] = True
                            return
                        if self.closed.is_set():
                            return
                        self.send(dst, frame, direction, state)
                        continue
                    self.send(dst, frame, direction, state)
                    ev['forwarded'] = True
                    self.events.append(ev)
                    if direction == 'response' and complete and self.mode == 'cut-after-message':
                        self.events.append({'fault': 'cut-after-complete-response-message', 'stream': stream})
                        # Let the SQL consumer observe the first batch before severing the stream.
                        time.sleep(1)
                        return
        except (OSError, AssertionError) as exc:
            self.events.append({'direction': direction, 'relayError': str(exc)})
        finally:
            for sock in (src, dst):
                try: sock.shutdown(socket.SHUT_RDWR)
                except OSError: pass
                sock.close()

    @staticmethod
    def record_count(raw):
        def number(pos):
            value = shift = 0
            while True:
                byte = raw[pos]; pos += 1; value |= (byte & 127) << shift
                if byte < 128: return value, pos
                shift += 7
        pos = count = 0
        while pos < len(raw):
            tag, pos = number(pos); field, wire = tag >> 3, tag & 7
            if wire == 0: _, pos = number(pos)
            elif wire == 2:
                length, pos = number(pos)
                if field == 2: count += 1
                pos += length
            else: raise AssertionError('unexpected response protobuf wire kind')
        return count

    def close(self):
        self.closed.set(); self.release.set(); self.listener.close()
        for sock in self.sockets:
            try: sock.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            sock.close()
        self.thread.join(timeout=2)
        for thread in self.threads: thread.join(timeout=2)
        assert not self.thread.is_alive() and not any(t.is_alive() for t in self.threads)
