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
        <title>AI Learning Content Copilot | Cyber-Matrix Engine</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;800&family=Inter:wght@400;500;600&display=swap');
            
            body {
                font-family: 'Inter', sans-serif;
            }
            .font-mono {
                font-family: 'JetBrains Mono', monospace;
            }
            .matrix-glow {
                box-shadow: 0 0 40px rgba(16, 185, 129, 0.12), inset 0 0 20px rgba(16, 185, 129, 0.03);
            }
            .prose strong { color: #34d399; font-weight: 600; }
            .prose h1, .prose h2, .prose h3 { 
                color: #6ee7b7; 
                border-bottom: 1px solid rgba(6, 78, 59, 0.6); 
                padding-bottom: 0.4rem; 
                margin-top: 1.5rem;
                font-family: 'JetBrains Mono', monospace;
            }
            .prose ul { list-style-type: square; color: #a7f3d0; }
            .prose li { margin-bottom: 0.5rem; }
            
            ::-webkit-scrollbar { width: 6px; }
            ::-webkit-scrollbar-track { background: #030704; }
            ::-webkit-scrollbar-thumb { background: #065f46; border-radius: 3px; }
            ::-webkit-scrollbar-thumb:hover { background: #047857; }
        </style>
    </head>
    <body class="bg-[#030704] text-emerald-100 min-h-screen flex flex-col items-center justify-between p-6 selection:bg-emerald-500 selection:text-black">
        
        <!-- Top Status Bar -->
        <div class="w-full max-w-5xl flex justify-between items-center text-xs font-mono text-emerald-500/70 border-b border-emerald-950 pb-3 mb-6">
            <div class="flex items-center gap-3">
                <span class="inline-block w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
                <span>NODE: US-EAST-GROQ-YOUTUBE-V1</span>
            </div>
            <div class="hidden sm:flex gap-6">
                <span>PARADIGM: FIRST-PRINCIPLES</span>
                <span>STATUS: OPTIMAL</span>
            </div>
        </div>

        <!-- Main Cyber-Matrix Panel -->
        <div class="w-full max-w-5xl bg-[#060f0a] border border-emerald-900/80 p-8 rounded-2xl matrix-glow relative overflow-hidden shadow-2xl">
            
            <!-- Top Gradient Accent Line -->
            <div class="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-emerald-600 via-green-400 to-teal-500"></div>

            <!-- Header Section -->
            <div class="flex flex-col md:flex-row md:items-center justify-between mb-8 border-b border-emerald-900/50 pb-6 gap-4">
                <div>
                    <h1 class="text-3xl font-extrabold tracking-tight bg-gradient-to-r from-emerald-400 via-green-300 to-teal-300 bg-clip-text text-transparent font-mono">
                        // AI_LEARNING_CONTENT_COPILOT
                    </h1>
                    <p class="text-xs text-emerald-500/90 mt-2 uppercase tracking-widest font-semibold font-mono">
                        Reality ➔ Measurement ➔ Models ➔ Understanding ➔ Better Decisions ➔ Action
                    </p>
                </div>
                <div class="flex items-center gap-3 self-start md:self-auto">
                    <span class="px-3.5 py-1.5 bg-emerald-950/80 text-emerald-400 border border-emerald-700/40 text-xs rounded-lg font-mono shadow-inner flex items-center gap-2">
                        <span class="w-2 h-2 rounded-full bg-emerald-400"></span>
                        AGENT ACTIVE
                    </span>
                </div>
            </div>
            
            <!-- Search & Action Input Bar -->
            <div class="flex flex-col sm:flex-row gap-3 mb-6">
                <div class="relative flex-1">
                    <span class="absolute inset-y-0 left-0 flex items-center pl-4 text-emerald-600 font-mono text-sm">></span>
                    <input type="text" id="queryInput" placeholder="Enter tech topic (e.g., Agentic workflows in Python)..." 
                        class="w-full bg-[#030704] border border-emerald-800/80 rounded-xl pl-9 pr-5 py-4 text-emerald-200 placeholder:text-emerald-700/60 focus:outline-none focus:border-emerald-400 focus:ring-1 focus:ring-emerald-400 transition-all text-sm font-mono shadow-inner">
                </div>
                <button onclick="runAnalysis()" id="submitBtn"
                    class="bg-emerald-600 hover:bg-emerald-500 active:scale-95 px-8 py-4 rounded-xl font-bold font-mono text-black transition-all shadow-lg shadow-emerald-900/40 flex items-center justify-center gap-2 cursor-pointer">
                    <span>EXECUTE BRIEF</span>
                    <span>⚡</span>
                </button>
            </div>

            <!-- Loading Indicator -->
            <div id="loader" class="hidden text-center py-16 text-emerald-400 font-mono text-xs tracking-widest bg-[#030704]/70 rounded-xl border border-emerald-900/40">
                <div class="inline-block animate-spin mr-2">⚙️</div> INGESTING YOUTUBE STREAMS, COMPUTING ENGAGEMENT RATIOS & SYNTHESIZING MASTER BRIEF...
            </div>

            <!-- Result Container -->
            <div id="resultWrapper" class="hidden">
                <div class="flex justify-between items-center mb-3 px-1">
                    <span class="text-xs font-mono text-emerald-500/80 uppercase tracking-wider">Generated Pedagogical Output</span>
                    <button onclick="copyBrief()" id="copyBtn" class="text-xs font-mono text-emerald-400 hover:text-emerald-300 bg-emerald-950/60 border border-emerald-800/60 px-3 py-1 rounded-md transition-all flex items-center gap-1.5 cursor-pointer">
                        <span>📋</span> <span>Copy Brief</span>
                    </button>
                </div>
                <div id="resultContainer" class="bg-[#030704] border border-emerald-900/70 rounded-xl p-6 prose prose-invert max-w-none text-emerald-100/90 shadow-inner font-sans text-sm leading-relaxed">
                </div>
            </div>
        </div>

        <!-- Footer -->
        <footer class="w-full max-w-5xl text-center text-xs font-mono text-emerald-700/60 mt-8 pt-4 border-t border-emerald-950">
            Autonomous AI Education Content Engine • Built for Hackathon Excellence
        </footer>

        <script>
            async function runAnalysis() {
                const query = document.getElementById('queryInput').value.trim();
                const loader = document.getElementById('loader');
                const resultWrapper = document.getElementById('resultWrapper');
                const resultContainer = document.getElementById('resultContainer');
                const btn = document.getElementById('submitBtn');

                if (!query) {
                    alert('Please enter a technical topic first.');
                    return;
                }

                loader.classList.remove('hidden');
                resultWrapper.classList.add('hidden');
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
                        resultWrapper.classList.remove('hidden');
                    } else {
                        resultContainer.innerHTML = `<span class="text-rose-400 font-mono">CRITICAL ERROR: ${data.detail}</span>`;
                        resultWrapper.classList.remove('hidden');
                    }
                } catch (err) {
                    alert('Network error: ' + err);
                } finally {
                    loader.classList.add('hidden');
                    btn.disabled = false;
                }
            }

            function copyBrief() {
                const text = document.getElementById('resultContainer').innerText;
                navigator.clipboard.writeText(text).then(() => {
                    const copyBtn = document.getElementById('copyBtn');
                    copyBtn.innerHTML = '<span>✅</span> <span>Copied!</span>';
                    setTimeout(() => {
                        copyBtn.innerHTML = '<span>📋</span> <span>Copy Brief</span>';
                    }, 2000);
                });
            }
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
