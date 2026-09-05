from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from googleapiclient.discovery import build

app = FastAPI(title="AI Learning Content Copilot Pipeline")

# ---------------------------------------
# CORS კონფიგურაცია
# ---------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------
# კონფიგურაცია (გასაღებები)
# ---------------------------------------
GROQ_API_KEY = ""
YOUTUBE_API_KEY = ""

client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=GROQ_API_KEY
)


# ---------------------------------------
# 1. Reality, Observation & Measurement Layer (Smart AI Tech Fetcher)
# ---------------------------------------
def optimize_query(user_query: str) -> str:
    """ოპტიმიზებს ქევორდებს მკაცრად AI სწავლებისა და ტექნიკური კონტენტისთვის"""
    q_lower = user_query.lower()
    tech_keywords = ["ai", "python", "agent", "rag", "llm", "machine learning", "code", "tutorial", "learn"]

    if not any(term in q_lower for term in tech_keywords):
        return user_query + " AI learning tutorial python code"
    return user_query + " tutorial guide 2026"

def fetch_youtube_data(query: str, max_results: int = 5) -> str:
    refined_query = optimize_query(query)
    print(f"1. Fetching live AI tech YouTube data for refined query: '{refined_query}'...")
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

        # ნაბიჯი 1: ვიდეოების ID-ების ძიება
        search_request = youtube.search().list(
            q=refined_query,
            part='id',
            maxResults=max_results,
            type='video'
        )
        search_response = search_request.execute()

        video_ids = [item['id']['videoId'] for item in search_response.get('items', []) if 'videoId' in item['id']]

        if not video_ids:
            return "No relevant AI tech videos found for this query."

        # ნაბიჯი 2: ზუსტი მეტრიკები Videos API-დან (Views, Likes, Comments)
        videos_request = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids)
        )
        videos_response = videos_request.execute()

        videos = []
        for item in videos_response.get('items', []):
            title = item['snippet']['title']
            video_id = item['id']
            views = item['statistics'].get('viewCount', 'N/A')
            likes = item['statistics'].get('likeCount', 'N/A')
            comments = item['statistics'].get('commentCount', 'N/A')
            description = item['snippet']['description'][:200]

            videos.append(
                f"Title: {title}\n"
                f"URL: https://www.youtube.com/watch?v={video_id}\n"
                f"Views: {views}\n"
                f"Likes: {likes}\n"
                f"Comments: {comments}\n"
                f"Description Snippet: {description}\n"
            )

        return "\n---\n".join(videos)
    except Exception as e:
        print(f"YouTube API Error: {e}")
        return f"Mock Data / Error: Unable to fetch live YouTube metrics. Details: {str(e)}"


# ---------------------------------------
# 2. Intelligence Layer (AI Education Copilot Agent)
# ---------------------------------------
def analyze_with_ai_agent(data_context: str, user_query: str) -> str:
    print("2. Running AI Education Copilot synthesis...")

    system_instruction = (
        "You are an elite Principal AI Architect, Lead Technical Educator, and Cognitive Science Researcher. "
        "Your task is to transform raw YouTube tech ingestion streams and hard metrics into a rigorous "
        "Pedagogical Brief and Content Strategy. Focus on technical depth, cognitive load management, "
        "and actionable execution blueprints for AI educators."
    )

    user_prompt = f"""
    Analyze the following AI education YouTube stream containing hard metrics for the query: '{user_query}'
    
    RAW DATA CONTEXT:
    {data_context}

    Structure your response rigorously using ONLY clean bullet points (NO MARKDOWN TABLES):

    ### 1. Top Performance & Technical Benchmarks
    - List the evaluated AI videos explicitly with their **Title, URL, View Count, Like Count, and Comment Count**.

    ### 2. Cognitive & Pedagogical Synthesis (Measurement ➔ Models)
    - **Content Depth vs. Engagement:** Which technical topic generated the highest engagement efficiency (Likes/Views ratio)?
    - **Friction Points:** What conceptual barriers or gaps are learners expressing in descriptions/metrics?

    ### 3. Actionable Master Brief (Better Decisions ➔ Action)
    - **The Hook & First 30 Seconds:** Provide a high-impact opening script strategy for this topic.
    - **Core Technical Blueprint:** Provide 2 key technical sub-topics that must be covered to satisfy audience demand.
    """

    try:
        response = client.chat.completions.create(
            model="openai/gpt-oss-20b",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=1400
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
# 3. Decision & Action Layer (API Endpoints)
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
# Meaningful Presentation Layer (Cyber-Matrix UI)
# ---------------------------------------
@app.get("/", response_class=HTMLResponse)
def presentation_ui():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>AI Learning Content Copilot</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
        <style>
            .matrix-glow {
                box-shadow: 0 0 25px rgba(34, 197, 94, 0.15);
            }
            .prose strong { color: #4ade80; }
            .prose h1, .prose h2, .prose h3 { color: #86efac; border-bottom: 1px solid #14532d; padding-bottom: 0.3rem; }
            .prose ul { list-style-type: square; color: #bbf7d0; }
        </style>
    </head>
    <body class="bg-[#050b07] text-emerald-100 min-h-screen flex flex-col items-center p-6 selection:bg-emerald-500 selection:text-black font-mono">
        
        <div class="w-full max-w-5xl bg-[#0a140f] border border-emerald-900/60 p-8 rounded-2xl matrix-glow relative overflow-hidden">
            
            <div class="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-emerald-500 via-green-400 to-teal-500"></div>

            <div class="flex items-center justify-between mb-8 border-b border-emerald-900/40 pb-5">
                <div>
                    <h1 class="text-3xl font-extrabold tracking-tight bg-gradient-to-r from-emerald-400 via-green-300 to-teal-400 bg-clip-text text-transparent">
                        // AI_LEARNING_CONTENT_COPILOT.v1
                    </h1>
                    <p class="text-xs text-emerald-500/80 mt-1 uppercase tracking-widest font-semibold">
                        Reality ➔ Observation ➔ Measurement ➔ Data ➔ Models ➔ Understanding ➔ Action
                    </p>
                </div>
                <div class="flex items-center gap-2">
                    <span class="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-ping"></span>
                    <span class="px-3 py-1 bg-emerald-950 text-emerald-400 border border-emerald-700/50 text-xs rounded-full font-mono shadow-inner">
                        SYS: ONLINE [AI COPILOT]
                    </span>
                </div>
            </div>
            
            <div class="flex gap-3 mb-8">
                <input type="text" id="queryInput" placeholder="Enter tech topic (e.g., Agentic workflows in Python)..." 
                    class="flex-1 bg-[#050b07] border border-emerald-800/80 rounded-xl px-5 py-4 text-emerald-200 placeholder:text-emerald-700/60 focus:outline-none focus:border-emerald-400 focus:ring-1 focus:ring-emerald-400 transition-all text-sm font-mono">
                <button onclick="runAnalysis()" id="submitBtn"
                    class="bg-emerald-600 hover:bg-emerald-500 active:scale-95 px-8 py-4 rounded-xl font-bold text-black transition-all shadow-lg shadow-emerald-900/50 flex items-center gap-2 cursor-pointer">
                    <span>GENERATE BRIEF</span>
                    <span>⚡</span>
                </button>
            </div>

            <div id="loader" class="hidden text-center py-20 text-emerald-400 animate-pulse font-mono text-sm tracking-widest bg-[#050b07]/50 rounded-xl border border-emerald-900/30">
                [>] INGESTING AI TECH STREAMS & COMPILING MASTER BRIEF...
            </div>

            <div id="resultContainer" class="hidden bg-[#050b07] border border-emerald-900/60 rounded-xl p-6 mt-4 prose prose-invert max-w-none text-emerald-200/90 shadow-inner font-sans">
            </div>
        </div>

        <script>
            async function runAnalysis() {
                const query = document.getElementById('queryInput').value.trim();
                const loader = document.getElementById('loader');
                const resultContainer = document.getElementById('resultContainer');
                const btn = document.getElementById('submitBtn');

                if (!query) return alert('Please enter a technical topic');

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
                        resultContainer.innerHTML = `<span class="text-rose-400 font-mono">CRITICAL ERROR: ${data.detail}</span>`;
                        resultContainer.classList.remove('hidden');
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
