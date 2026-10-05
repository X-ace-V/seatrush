import Fastify from 'fastify';

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? 'info' } });

app.get('/health', async () => ({ ok: true }));

await app.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
