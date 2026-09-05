from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from googleapiclient.discovery import build

app = FastAPI(title="YouTube AI Agent Pipeline API")

# ---------------------------------------
# CORS კონფიგურაცია (რომ ინტერფეისმა იმუშაოს)
# ---------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------
# კონფიგურაცია (ჩასვი შენი გასაღებები)
# ---------------------------------------
GROQ_API_KEY = "xxx"
YOUTUBE_API_KEY = "xxx"

client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=GROQ_API_KEY
)


# ---------------------------------------
# YouTube Data Fetcher
# ---------------------------------------
def fetch_youtube_data(query: str, max_results: int = 5) -> str:
    print(f"1. Fetching live YouTube data for: '{query}'...")
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        request = youtube.search().list(
            q=query,
            part='snippet',
            maxResults=max_results,
            type='video'
        )
        response = request.execute()

        videos = []
        for item in response.get('items', []):
            title = item['snippet']['title']
            video_id = item['id']['videoId']
            description = item['snippet']['description']
            videos.append(f"Title: {title}\nURL: https://www.youtube.com/watch?v={video_id}\nDescription: {description}\n")

        if not videos:
            return "No videos found for this query."

        return "\n---\n".join(videos)
    except Exception as e:
        print(f"YouTube API Error: {e}")
        return f"Mock Data / Error: Unable to fetch live YouTube data for query: {query}. Details: {str(e)}"


# ---------------------------------------
# AI Agent Analyzer (მოკლე, ოპტიმიზებული პრომპტით)
# ---------------------------------------
def analyze_with_ai_agent(data_context: str, user_query: str) -> str:
    print("2. Sending data to AI Agent (Groq)...")

    prompt = f"""
    Analyze YouTube data for '{user_query}':
    {data_context}

    1. FILTER: Drop noise/false positives. State Signal-to-Noise %.
    2. CLUSTER: Group valid results into clean content categories.
    3. INSIGHTS: Extract strict patterns and actionable recommendations from verified data only. No hallucinations.
    """

    try:
        response = client.chat.completions.create(
            model="openai/gpt-oss-20b",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Groq API Error: {e}")
        raise HTTPException(status_code=500, detail=f"AI Agent Error: {str(e)}")


def run_pipeline(user_query: str, max_results: int = 5) -> str:
    raw_data = fetch_youtube_data(user_query, max_results)
    ai_analysis = analyze_with_ai_agent(raw_data, user_query)
    return ai_analysis


# ---------------------------------------
# API ენდპოინტები
# ---------------------------------------
@app.post("/analyze")
def analyze_endpoint(query: str = Body(..., media_type="text/plain")):
    if not query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")
    try:
        analysis_result = run_pipeline(query, max_results=5)
        return {"query": query, "analysis": analysis_result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")


# ---------------------------------------
# პრეზენტაციის ვიზუალური ინტერფეისი (HTML / Tailwind UI)
# ---------------------------------------
@app.get("/", response_class=HTMLResponse)
def presentation_ui():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>YouTube AI Analytics Agent</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    </head>
    <body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col items-center p-6 selection:bg-blue-500 selection:text-white">
        <div class="w-full max-w-5xl bg-slate-900 border border-slate-800 p-8 rounded-2xl shadow-2xl">
            <div class="flex items-center justify-between mb-6 border-b border-slate-800 pb-4">
                <div>
                    <h1 class="text-2xl font-bold bg-gradient-to-r from-blue-400 to-indigo-400 bg-clip-text text-transparent">
                        YouTube AI Analytics Agent
                    </h1>
                    <p class="text-xs text-slate-400 mt-1">Reality ➔ Observation ➔ Measurement ➔ Data ➔ Models ➔ Understanding ➔ Action</p>
                </div>
                <span class="px-3 py-1 bg-blue-500/10 text-blue-400 border border-blue-500/20 text-xs rounded-full font-mono">FastAPI + Groq Llama</span>
            </div>
            
            <div class="flex gap-3 mb-6">
                <input type="text" id="queryInput" placeholder="Enter topic (e.g., German food trends 2025)..." 
                    class="flex-1 bg-slate-950 border border-slate-800 rounded-xl px-4 py-3 text-slate-100 placeholder:text-slate-600 focus:outline-none focus:border-blue-500 transition-colors">
                <button onclick="runAnalysis()" id="submitBtn"
                    class="bg-blue-600 hover:bg-blue-500 active:scale-95 px-6 py-3 rounded-xl font-semibold transition-all shadow-lg shadow-blue-600/20">
                    Run Pipeline
                </button>
            </div>

            <div id="loader" class="hidden text-center py-16 text-blue-400 animate-pulse font-mono text-sm">
                ⚡ Fetching YouTube Data & Executing AI Analysis...
            </div>

            <div id="resultContainer" class="hidden bg-slate-950 border border-slate-800 rounded-xl p-6 mt-4 prose prose-invert max-w-none text-slate-300">
            </div>
        </div>

        <script>
            async function runAnalysis() {
                const query = document.getElementById('queryInput').value.trim();
                const loader = document.getElementById('loader');
                const resultContainer = document.getElementById('resultContainer');
                const btn = document.getElementById('submitBtn');

                if (!query) return alert('Please enter a query');

                loader.classList.remove('hidden');
                resultContainer.classList.add('hidden');
                btn.disabled = true;

                try {
                    const response = await fetch('/analyze', {
                        method: 'POST',
                        headers: { 'Content-Type': 'text/plain' },
                        body: query
                    });

                    const data = await response.json();
                    if (response.ok) {
                        resultContainer.innerHTML = marked.parse(data.analysis);
                        resultContainer.classList.remove('hidden');
                    } else {
                        resultContainer.innerHTML = `<span class="text-red-400">Error: ${data.detail}</span>`;
                        resultContainer.classList.add('hidden');
                    }
                } catch (err) {
                    alert('Network error: ' + err);
                } finally {
                    loader.classList.add('hidden');
                    btn.disabled = false;
                }
            }
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
