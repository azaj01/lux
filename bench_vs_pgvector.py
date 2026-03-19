import socket
import time
import random
import subprocess
import psycopg2
import sys

DIM = 1536
COUNTS = [100, 1000, 5000, 10000]
K = 10
QUERIES = 50

def random_vector(dim):
    return [random.gauss(0, 1) for _ in range(dim)]

def send_resp(sock, args):
    cmd = f'*{len(args)}\r\n'
    for a in args:
        s = str(a)
        cmd += f'${len(s)}\r\n{s}\r\n'
    sock.sendall(cmd.encode())

def read_resp(sock):
    data = b''
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        data += chunk
        if data.endswith(b'\r\n'):
            break
    return data

def bench_lux(n, dim, query_vec):
    sock = socket.socket()
    sock.settimeout(60)
    sock.connect(('localhost', 6399))

    send_resp(sock, ['FLUSHALL'])
    read_resp(sock)

    vecs = [random_vector(dim) for _ in range(n)]

    start = time.time()
    for i, v in enumerate(vecs):
        args = ['VSET', f'v:{i}', str(dim)] + [str(round(x, 6)) for x in v]
        send_resp(sock, args)
        read_resp(sock)
    insert_time = time.time() - start

    q_args = ['VSEARCH', str(dim)] + [str(round(x, 6)) for x in query_vec] + ['K', str(K)]
    times = []
    for _ in range(QUERIES):
        start = time.time()
        send_resp(sock, q_args)
        read_resp(sock)
        times.append((time.time() - start) * 1000)

    sock.close()

    times.sort()
    return {
        'insert_sec': insert_time,
        'insert_rate': n / insert_time,
        'p50': times[len(times) // 2],
        'p99': times[int(len(times) * 0.99)],
        'avg': sum(times) / len(times),
    }

def bench_pgvector(n, dim, query_vec):
    conn = psycopg2.connect("postgresql://postgres:postgres@127.0.0.1:54422/postgres")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS bench_vectors")
    cur.execute(f"CREATE TABLE bench_vectors (id serial PRIMARY KEY, embedding vector({dim}), meta text)")
    cur.execute("CREATE INDEX ON bench_vectors USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)")

    vecs = [random_vector(dim) for _ in range(n)]

    start = time.time()
    for i, v in enumerate(vecs):
        vec_str = '[' + ','.join(str(round(x, 6)) for x in v) + ']'
        cur.execute("INSERT INTO bench_vectors (embedding, meta) VALUES (%s, %s)", (vec_str, f'{{"i":{i}}}'))
    insert_time = time.time() - start

    q_str = '[' + ','.join(str(round(x, 6)) for x in query_vec) + ']'

    cur.execute("ANALYZE bench_vectors")

    times = []
    for _ in range(QUERIES):
        start = time.time()
        cur.execute(
            f"SELECT id, 1 - (embedding <=> %s::vector) AS similarity FROM bench_vectors ORDER BY embedding <=> %s::vector LIMIT {K}",
            (q_str, q_str)
        )
        cur.fetchall()
        times.append((time.time() - start) * 1000)

    cur.execute("DROP TABLE bench_vectors")
    conn.close()

    times.sort()
    return {
        'insert_sec': insert_time,
        'insert_rate': n / insert_time,
        'p50': times[len(times) // 2],
        'p99': times[int(len(times) * 0.99)],
        'avg': sum(times) / len(times),
    }

print(f"Vector Search Benchmark: Lux VSEARCH vs pgvector")
print(f"Dimensions: {DIM}, K: {K}, Queries: {QUERIES}")
print(f"{'='*70}")
print(f"{'Vectors':>8} | {'Engine':>10} | {'Insert/s':>10} | {'p50 ms':>8} | {'p99 ms':>8} | {'avg ms':>8}")
print(f"{'-'*70}")

for n in COUNTS:
    query = random_vector(DIM)

    lux = bench_lux(n, DIM, query)
    print(f"{n:>8} | {'Lux':>10} | {lux['insert_rate']:>10.0f} | {lux['p50']:>8.2f} | {lux['p99']:>8.2f} | {lux['avg']:>8.2f}")

    pg = bench_pgvector(n, DIM, query)
    print(f"{n:>8} | {'pgvector':>10} | {pg['insert_rate']:>10.0f} | {pg['p50']:>8.2f} | {pg['p99']:>8.2f} | {pg['avg']:>8.2f}")

    speedup = pg['avg'] / lux['avg'] if lux['avg'] > 0 else 0
    print(f"{'':>8} | {'Lux faster':>10} | {'':>10} | {pg['p50']/lux['p50']:>7.1f}x | {pg['p99']/lux['p99']:>7.1f}x | {speedup:>7.1f}x")
    print(f"{'-'*70}")
