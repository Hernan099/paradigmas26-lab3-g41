# Reddit Mock Server

A lightweight mock HTTP server that simulates the Reddit API for testing and development purposes. This server serves pre-recorded Reddit posts from JSON files without requiring actual network calls to Reddit.

## Purpose

This mock server allows the lab exercises to:
- Run tests and demonstrations without external API dependencies
- Ensure consistent, reproducible data across runs
- Avoid rate limiting and API access issues
- Provide fast, local development and testing experience

## Building the Server

```bash
cd reddit-mock
sbt run
```

## Running the Server

Once built, the server will:
1. Start an HTTP server on `http://localhost:8123`
2. Scan the `subreddits/` directory for `.json` files
3. Create endpoints for each subreddit in the format: `/r/{subreddit}/.json`
4. Print available subreddits to the console

### Example Output

```
==================================================
Fake Reddit API running on http://localhost:8123
==================================================
Available subreddits (4):
--------------------------------------------------
  r/computerscience
  r/learnpython
  r/programming
  r/scala
==================================================
Press Ctrl+C (or stop the process) to shut down.
```

## Available Endpoints

The server provides the following endpoints based on the JSON files in `subreddits/`:

- `http://localhost:8123/r/computerscience/.json`
- `http://localhost:8123/r/learnpython/.json`
- `http://localhost:8123/r/programming/.json`
- `http://localhost:8123/r/scala/.json`

Each endpoint returns the corresponding subreddit's posts as JSON.

## Response Format

Responses follow the Reddit API format with an HTTP status code:
- **200 OK**: Subreddit data successfully returned
- **404 Not Found**: Requested subreddit not found

Sample response headers:
```
Content-Type: application/json
```

## Implementation Details

- **Framework**: Java's built-in `HttpServer`
- **Language**: Scala 2.13.18
- **Dependencies**: json4s-jackson 4.0.6
- **Response Delay**: 5 seconds (simulates network latency)

## Data Files

The `subreddits/` directory contains pre-recorded JSON files from real Reddit posts:

| File | Description |
|------|-------------|
| `computerscience.json` | Posts from r/computerscience |
| `learnpython.json` | Posts from r/learnpython |
| `programming.json` | Posts from r/programming |
| `scala.json` | Posts from r/scala |

## Adding More Subreddits

To add a new subreddit:

1. Create a JSON file in the `subreddits/` directory with the format: `{subredditname}.json`
2. Populate it with posts in Reddit API format
3. Restart the server

The new subreddit endpoint will be automatically available at `/r/{subredditname}/.json`.

## Usage in Lab Exercises

The skeleton and Spark solutions reference this mock server:

```scala
// Example: Download posts from the mock server
val url = "http://localhost:8123/r/scala/.json"
val posts = downloadFeed(url)
```

## Shutdown

To stop the server:
- Press `Ctrl+C` in the terminal where the server is running
- The server will gracefully shut down after completing in-flight requests

## Troubleshooting

**Port already in use**: The server uses port `8123`. If this port is in use:
- Find and stop the process using that port
- Or modify the `port` variable in `MockRedditApi.scala`

**Subreddits not showing**: Ensure JSON files are in the `subreddits/` directory and named with the `.json` extension.

**Connection refused**: Verify the server is running and accessible at `localhost:8123`.
