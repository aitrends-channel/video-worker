# Video Worker Service

Background worker service for processing video generation and assembly jobs. Runs video generation queues, handles FFmpeg assembly, and manages long-running tasks that don't fit in Vercel serverless functions.

## Architecture

- **BullMQ Queue**: Persistent job queue backed by Redis
- **Video Worker**: Processes KIE.ai video generation jobs
- **Express Server**: Health checks and monitoring endpoints

## Features

- ✅ Video generation job processing
- ✅ Supabase database updates
- ✅ Storage uploads
- ✅ Graceful shutdown handling
- ✅ Concurrency control (3 concurrent jobs by default)
- ✅ Exponential backoff retry for API calls

## Prerequisites

- Node.js 18+
- Redis instance (Upstash recommended for serverless)
- Supabase account and project
- KIE.ai, ElevenLabs, and Anthropic API keys

## Installation

```bash
npm install
```

## Configuration

Copy `.env.example` to `.env.local` and fill in all required values:

```bash
cp .env.example .env.local
```

### Required Environment Variables

```env
# Redis / Upstash
UPSTASH_REDIS_REST_URL=https://xxxx.upstash.io
UPSTASH_REDIS_REST_TOKEN=xxxxx
UPSTASH_REDIS_HOST=xxxx.upstash.io
UPSTASH_REDIS_PORT=6379
UPSTASH_REDIS_PASSWORD=xxxxx
UPSTASH_REDIS_TLS=true

# Supabase
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJxxxxx

# External APIs
KIE_API_KEY=kie-xxxx
ELEVENLABS_API_KEY=sk_xxxx
ANTHROPIC_API_KEY=sk-ant-xxxx

# Server
PORT=3001
NODE_ENV=production
```

## Development

```bash
npm run dev
```

Server runs on `http://localhost:3001`

## Build

```bash
npm run build
```

## Production Deployment

### Option 1: Railway

1. Connect GitHub repo to Railway
2. Add environment variables in Railway dashboard
3. Set start command: `npm run build && npm start`

```bash
# Or deploy directly
railway up
```

### Option 2: Render

1. Create new Web Service on Render
2. Connect GitHub repo
3. Build command: `npm run build`
4. Start command: `npm start`
5. Add environment variables

### Option 3: Fly.io

```bash
flyctl launch
flyctl secrets set $(cat .env.local)
flyctl deploy
```

### Option 4: Self-hosted / VPS

```bash
# Build
npm run build

# Start with process manager (pm2 recommended)
npm install -g pm2
pm2 start npm --name "video-worker" -- start
pm2 save
```

## API Endpoints

### Health Check
```bash
GET /health
```

Response:
```json
{
  "status": "ok",
  "timestamp": "2026-05-07T12:00:00.000Z",
  "uptime": 1234.56
}
```

### Ready Check
```bash
GET /health/ready
```

### Worker Status
```bash
GET /api/worker/status
```

Response:
```json
{
  "worker": "video-generation",
  "running": true,
  "concurrency": 3,
  "timestamp": "2026-05-07T12:00:00.000Z"
}
```

## Job Submission

From your Vercel apps (auth-api, youtube-engine), submit jobs to the queue:

```typescript
import { videoQueue } from './lib/queue/client';

// Submit video job
await videoQueue.add('generate-video', {
  projectId: 'proj-123',
  beatNumber: 1,
  videoPrompt: 'A sunset over mountains',
  imageUrl: 'https://...',
  modelId: 'kie-video-1',
  duration: 5,
  aspectRatio: '16:9'
});
```

## Monitoring

- **Upstash Dashboard**: Monitor Redis queue at https://console.upstash.com
- **Server Logs**: Check application logs in your hosting platform
- **Health Endpoints**: Poll `/health` or `/health/ready` for readiness

## Troubleshooting

### Worker not picking up jobs
- Check Redis connection: `UPSTASH_REDIS_REST_URL` and credentials
- Verify BullMQ queue name matches in Vercel apps
- Check worker logs for connection errors

### Video upload failures
- Verify `SUPABASE_SERVICE_ROLE_KEY` has storage permissions
- Check Supabase Storage "media" bucket exists
- Ensure storage bucket is public

### API rate limits
- KIE.ai: Built-in exponential backoff with 60 max attempts
- ElevenLabs: Handle 429 responses in assembly route
- Implement queuing to avoid overwhelming external APIs

## Architecture Notes

This worker service is **completely separate** from your Vercel apps:

```
Vercel (auth-api + youtube-engine)
        ↓
    Redis Queue
        ↓
   video-worker (this service)
        ↓
    Supabase + Storage
```

- Vercel apps submit jobs and poll status
- Worker processes jobs independently
- Both read/write to shared Supabase database
- No direct API calls between them

## License

MIT


## TEMPORAL PRODUCTION URLS
youtube-engine app on vercel:
 https://aitrends-youtube-engine-git-vercel-rea-81a838-bansolos-projects.vercel.app/