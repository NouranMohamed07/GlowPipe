import { useState, useEffect } from "react";

const API_BASE = "http://localhost:8000";

const SKIN_TYPES = ["oily", "dry", "sensitive", "combination", "normal"];

const PREFERENCE_OPTIONS = [
  { key: "vegan", label: " Vegan" },
  { key: "cruelty_free", label: " Cruelty Free" },
  { key: "fragrance_free", label: " Fragrance Free" },
  { key: "alcohol_free", label: " Alcohol Free" },
  { key: "paraben_free", label: " Paraben Free" },
  { key: "sulfate_free", label: "Sulfate Free" },
  { key: "silicone_free", label: " Silicone Free" },
  { key: "oil_free", label: " Oil Free" },
  { key: "reef_safe", label: " Reef Safe" },
  { key: "pregnancy_safe", label: " Pregnancy Safe" },
  { key: "fungal_acne_safe", label: " Fungal Acne Safe" },
];

const BADGE_COLORS = {
  "Vegan": "#a8d5a2", "Cruelty Free": "#f5c6d0", "Reef Safe": "#a2c8d5",
  "Fragrance Free": "#e8d5f5", "Alcohol Free": "#d5e8f5", "Paraben Free": "#f5e8d5",
  "Sulfate Free": "#d5f5e8", "Silicone Free": "#f5d5e8", "Oil Free": "#e8f5d5",
  "Pregnancy Safe": "#f5f0d5", "Fungal Acne Safe": "#d5d5f5",
};

// ── Real categories & subcategories from GLOWPIPE_DB.GOLD.VIEW_RECOMMENDATION_PRODUCTS ──
const CATEGORY_MAP = {
  "All Categories": [],

  // ── Main categories from DIM_CATEGORY ──
  "Skincare": [
    "All Subcategories",
    "Treatments & Actives",
    "Moisturizers",
    "Cleansers",
    "Toners",
    "Sunscreens",
    "Makeup Removers",
    "Eye Care",
    "Exfoliators",
    "Masks",
    "Prescription & Clinical",
    "Lip Care",
  ],
  "Body Care": [
    "All Subcategories",
    "Body Lotions & Moisturizers",
    "Bath & Body Washes",
    "Body Scrubs",
    "Hand & Foot Care",
  ],
  "Face Makeup": [
    "All Subcategories",
  ],
  "Hair Care": [
    "All Subcategories",
    "Hair Treatments",
    "Shampoos & Conditioners",
  ],
  "Eye Makeup": [
    "All Subcategories",
  ],
  "Lip Makeup": [
    "All Subcategories",
  ],
};

// Flat list of ALL subcategories for quick lookup
const ALL_SUBCATEGORIES = [
  "Treatments & Actives",
  "Moisturizers",
  "Cleansers",
  "Toners",
  "Sunscreens",
  "Makeup Removers",
  "Eye Care",
  "Exfoliators",
  "Masks",
  "Prescription & Clinical",
  "Body Lotions & Moisturizers",
  "Lip Care",
  "Hair Treatments",
  "Bath & Body Washes",
  "Shampoos & Conditioners",
  "Body Scrubs",
  "Hand & Foot Care",
];

function StarRating({ rating }) {
  return (
    <div style={{ display: "flex", gap: "2px", alignItems: "center" }}>
      {[1,2,3,4,5].map(s => (
        <span key={s} style={{ fontSize: "12px", color: s <= Math.round(rating) ? "#e8a0b4" : "#e0d0d8" }}>★</span>
      ))}
      <span style={{ fontSize: "17px", color: "#b09aa8", marginLeft: "4px" }}>{rating?.toFixed(1)}</span>
    </div>
  );
}

function Badge({ label }) {
  return (
    <span style={{
      background: BADGE_COLORS[label] || "#f0e8f0", color: "#5a3a5a",
      fontSize: "15px", fontFamily: "'Cormorant Garamond', Georgia, serif",
      fontWeight: "800", padding: "3px 8px", borderRadius: "20px",
      letterSpacing: "0.03em", display: "inline-block",
    }}>{label}</span>
  );
}

function ScoreBar({ value, label, color = "#e8a0b4" }) {
  return (
    <div style={{ marginBottom: "6px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "3px" }}>
        <span style={{ fontSize: "16px", color: "#b09aa8", fontFamily: "'Cormorant Garamond', Georgia, serif" }}>{label}</span>
        <span style={{ fontSize: "16px", color: "#8a6a7a", fontWeight: "800" }}>{(value * 100).toFixed(0)}%</span>
      </div>
      <div style={{ background: "#f0e8ee", borderRadius: "10px", height: "5px", overflow: "hidden" }}>
        <div style={{ width: `${value * 100}%`, height: "100%", background: `linear-gradient(90deg, ${color}, ${color}cc)`, borderRadius: "10px", transition: "width 0.8s ease" }} />
      </div>
    </div>
  );
}

function IngredientCard({ ing }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ background: "#fdf8fc", border: "1px solid #f0e0ec", borderRadius: "12px", padding: "10px 12px", marginBottom: "6px", cursor: "pointer" }}
      onClick={() => setOpen(o => !o)}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <span style={{ fontSize: "18px", fontWeight: "800", color: "#7a4a6a", fontFamily: "'Cormorant Garamond', Georgia, serif" }}>
            #{ing.ingredient_position} {ing.ingredient_name}
          </span>
          {ing.callout_type && (
            <span style={{ marginLeft: "6px", fontSize: "16px", background: "#f5d5e8", color: "#7a3a5a", padding: "2px 6px", borderRadius: "10px" }}>{ing.callout_type}</span>
          )}
        </div>
        {ing.evidence_level && <span style={{ fontSize: "16px", color: "#c090a8" }}>{ing.evidence_level}</span>}
      </div>
      {open && <p style={{ margin: "8px 0 0", fontSize: "16px", color: "#9a7a88", lineHeight: "1.5", fontStyle: "italic" }}>{ing.description || "No detailed description available."}</p>}
      {ing.warning_type && <div style={{ marginTop: "4px", fontSize: "16px", color: "#c05050", background: "#fff0f0", padding: "2px 6px", borderRadius: "6px", display: "inline-block" }}>⚠️ {ing.warning_type}</div>}
    </div>
  );
}

function ProductCard({ product, onSimilar }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div style={{
      background: "linear-gradient(135deg, #fffcfe 0%, #fdf5fa 100%)",
      border: "1px solid #f0dcea", borderRadius: "20px", overflow: "hidden",
      transition: "transform 0.2s, box-shadow 0.2s",
      boxShadow: "0 4px 20px rgba(200,150,180,0.08)",
      fontFamily: "'Cormorant Garamond', Georgia, serif",
    }}
    onMouseEnter={e => { e.currentTarget.style.transform = "translateY(-3px)"; e.currentTarget.style.boxShadow = "0 12px 32px rgba(200,150,180,0.18)"; }}
    onMouseLeave={e => { e.currentTarget.style.transform = "none"; e.currentTarget.style.boxShadow = "0 4px 20px rgba(200,150,180,0.08)"; }}
    >
      <div style={{ position: "relative", height: "180px", background: "linear-gradient(135deg, #fce8f3, #f0e8ff)", overflow: "hidden" }}>
        {product.image_url
          ? <img src={product.image_url} alt={product.product_name} style={{ width: "100%", height: "100%", objectFit: "contain", padding: "12px" }} onError={e => { e.target.style.display = "none"; }} />
          : <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", fontSize: "48px" }}>🧴</div>
        }
        <div style={{ position: "absolute", top: "10px", right: "10px", background: "rgba(255,255,255,0.9)", borderRadius: "20px", padding: "4px 10px", fontSize: "12px", color: "#7a4a6a", fontWeight: "700" }}>
          {product.final_score ? `${(product.final_score * 100).toFixed(0)}%` : ""} match
        </div>
        {/* Category badge top-left */}
        {product.sub_category && (
          <div style={{ position: "absolute", top: "10px", left: "10px", background: "rgba(232,160,180,0.9)", borderRadius: "20px", padding: "3px 8px", fontSize: "10px", color: "white", fontWeight: "600" }}>
            {product.sub_category}
          </div>
        )}
      </div>

      <div style={{ padding: "16px" }}>
        <div style={{ marginBottom: "8px" }}>
          <div style={{ fontSize: "17px", color: "#c090a8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "2px" }}>{product.brand_name}</div>
          <div style={{ fontSize: "19px", fontWeight: "800", color: "#4a2a4a", lineHeight: "1.3" }}>{product.product_name}</div>
          <div style={{ fontSize: "17px", color: "#c090a8", marginTop: "2px" }}>{product.category} · {product.sub_category}</div>
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "10px" }}>
          <StarRating rating={product.rating} />
          <span style={{ fontSize: "20px", fontWeight: "800", color: "#7a3a6a" }}>{product.currency || "$"}{product.price?.toFixed(2)}</span>
        </div>

        {product.skin_match_score !== undefined && (
          <div style={{ marginBottom: "10px" }}>
            <ScoreBar value={product.skin_match_score} label="Skin Match" color="#e8a0b4" />
            <ScoreBar value={product.safety_score} label="Safety" color="#a0c8a0" />
            <ScoreBar value={product.preference_match_score} label="Preference Match" color="#a0b4e8" />
          </div>
        )}

        {product.badges?.length > 0 && (
          <div style={{ display: "flex", flexWrap: "wrap", gap: "4px", marginBottom: "10px" }}>
            {product.badges.slice(0, 5).map(b => <Badge key={b} label={b} />)}
            {product.badges.length > 5 && <Badge label={`+${product.badges.length - 5}`} />}
          </div>
        )}

        {product.reason && (
          <div style={{ background: "linear-gradient(135deg, #fff0f8, #f8f0ff)", borderRadius: "10px", padding: "8px 10px", marginBottom: "10px", fontSize: "11px", color: "#8a5a78", lineHeight: "1.5", fontStyle: "italic" }}>
            💬 {product.reason}
          </div>
        )}

        <button onClick={() => setExpanded(!expanded)} style={{
          width: "100%", background: "none", border: "1px solid #f0dcea",
          borderRadius: "10px", padding: "7px", cursor: "pointer",
          fontSize: "16px", color: "#b07090", fontFamily: "'Cormorant Garamond', Georgia, serif", marginBottom: "6px",
        }}>
          {expanded ? "▲ Hide" : "▼ Show"} Top Ingredients
        </button>

        {expanded && product.top_3_ingredients?.length > 0 && (
          <div style={{ marginBottom: "8px" }}>
            {product.top_3_ingredients.map((ing, i) => <IngredientCard key={i} ing={ing} />)}
          </div>
        )}

        <div style={{ display: "flex", gap: "6px" }}>
          <a href={product.product_url} target="_blank" rel="noreferrer" style={{
            flex: 1, textAlign: "center", background: "linear-gradient(135deg, #e8a0b4, #c880a0)",
            color: "white", padding: "8px", borderRadius: "10px", fontSize: "11px",
            textDecoration: "none", fontWeight: "800", letterSpacing: "0.05em",
          }}>View Product</a>
          <button onClick={() => onSimilar(product.product_id)} style={{
            flex: 1, background: "none", border: "1px solid #e8a0b4",
            color: "#c880a0", padding: "8px", borderRadius: "10px", fontSize: "11px",
            cursor: "pointer", fontWeight: "800", fontFamily: "'Cormorant Garamond', Georgia, serif",
          }}>Similar ✨</button>
        </div>
      </div>
    </div>
  );
}

// ── SELECT STYLE helper ───────────────────────────────────────────────────────
const selectStyle = {
  width: "100%", padding: "8px 12px", borderRadius: "12px",
  border: "1px solid #f0dcea", background: "white",
  fontSize: "17px", color: "#4a2a4a", outline: "none",
  fontFamily: "'Cormorant Garamond', Georgia, serif",
  cursor: "pointer",
};

// ─────────────────────────────────────────────────────────────────────────────
export default function SkincareApp() {
  const [skinType, setSkinType] = useState("oily");
  const [acneProne, setAcneProne] = useState(false);
  const [budget, setBudget] = useState(100);
  const [topN, setTopN] = useState(6);
  const [preferences, setPreferences] = useState({});

  // ── NEW: category & subcategory filters ──────────────────────────────────
  const [selectedCategory, setSelectedCategory]       = useState("All Categories");
  const [selectedSubCategory, setSelectedSubCategory] = useState("All Subcategories");
  const [availableSubCats, setAvailableSubCats]        = useState([]);

  const [results, setResults]           = useState([]);
  const [loading, setLoading]           = useState(false);
  const [error, setError]               = useState(null);
  const [health, setHealth]             = useState(null);
  const [searchMode, setSearchMode]     = useState("recommend");
  const [similarProductId, setSimilarProductId] = useState(null);

  // When category changes → update subcategory list
  useEffect(() => {
    if (selectedCategory === "All Categories") {
      setAvailableSubCats([]);
      setSelectedSubCategory("All Subcategories");
    } else {
      const subs = CATEGORY_MAP[selectedCategory] || ["All Subcategories"];
      setAvailableSubCats(subs);
      setSelectedSubCategory("All Subcategories");
    }
  }, [selectedCategory]);

  const togglePref = (key) => setPreferences(p => ({ ...p, [key]: !p[key] }));

  const fetchHealth = async () => {
    try {
      const res = await fetch(`${API_BASE}/health`);
      setHealth(await res.json());
    } catch { setHealth({ status: "unreachable" }); }
  };

  const fetchRecommendations = async () => {
    setLoading(true); setError(null); setResults([]);
    try {
      const body = {
        skin_type: skinType,
        acne_prone: acneProne,
        max_budget: budget,
        top_n: topN,
        // ── NEW: send category filters to backend ──
        category: selectedCategory === "All" ? null : selectedCategory,
        sub_category: (selectedSubCategory === "All Subcategories" || selectedSubCategory === "") ? null : selectedSubCategory,
        preferences: Object.fromEntries(
          Object.entries(preferences).map(([k, v]) => [k, v || false])
        ),
      };
      const res = await fetch(`${API_BASE}/recommend`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setResults(data.recommendations || data);
      setSearchMode("recommend");
    } catch (e) { setError(e.message); }
    finally { setLoading(false); }
  };

  const fetchSimilar = async (productId) => {
    setLoading(true); setError(null); setResults([]);
    setSimilarProductId(productId);
    try {
      const res = await fetch(`${API_BASE}/similar-products`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ product_id: productId, top_n: topN }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setResults(data.similar_products || data);
      setSearchMode("similar");
    } catch (e) { setError(e.message); }
    finally { setLoading(false); }
  };

  // ── filtered results for display (client-side fallback) ──────────────────
  const displayResults = results.filter(p => {
    if (selectedCategory !== "All Categories" && p.category !== selectedCategory) return false;
    if (selectedSubCategory !== "All Subcategories" && selectedSubCategory !== "" && p.sub_category !== selectedSubCategory) return false;
    return true;
  });

  return (
    <div style={{ minHeight: "100vh", background: "linear-gradient(160deg, #fff8fd 0%, #fdf0f8 40%, #f8f0ff 100%)", fontFamily: "'Cormorant Garamond', Georgia, serif" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,600;0,700;1,300;1,400&display=swap');
        * { box-sizing: border-box; } body { margin: 0; }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #fdf0f8; }
        ::-webkit-scrollbar-thumb { background: #e8a0b4; border-radius: 3px; }
        select, input { font-family: inherit; }
        select:focus { border-color: #e8a0b4 !important; box-shadow: 0 0 0 3px rgba(232,160,180,0.15); }
      `}</style>

      {/* Hero */}
      <div style={{ background: "linear-gradient(135deg, #f7d6e8 0%, #edd5f5 50%, #d5e0f5 100%)", padding: "40px 24px 32px", textAlign: "center", position: "relative", overflow: "hidden" }}>
        {["#f0a0c0","#c8a0e0","#a0b8e0"].map((c,i) => (
          <div key={i} style={{ position: "absolute", width: `${120+i*80}px`, height: `${120+i*80}px`, borderRadius: "50%", background: c, opacity: "0.12", top: `${-30+i*10}px`, right: `${-20+i*60}px` }} />
        ))}
        <div style={{ position: "relative", zIndex: 1 }}>
          <div style={{ fontSize: "32px", marginBottom: "6px" }}>🌸</div>
          <h1 style={{ margin: "0 0 6px", fontSize: "clamp(28px,5vw,44px)", fontWeight: "500", color: "#4a2a4a", letterSpacing: "-0.02em" }}>
            GlowPipe <span style={{ fontStyle: "italic", color: "#9a5a8a" }}>Skin</span>
          </h1>
          <p style={{ margin: "0 0 16px", fontSize: "18px", color: "#9a7a88", fontWeight: "500", letterSpacing: "0.08em", textTransform: "uppercase" }}>
            Personalized Skincare Recommendations
          </p>
          <button onClick={fetchHealth} style={{ background: "rgba(255,255,255,0.7)", border: "1px solid rgba(232,160,180,0.4)", borderRadius: "20px", padding: "6px 16px", cursor: "pointer", fontSize: "11px", color: "#b07090", letterSpacing: "0.05em", backdropFilter: "blur(8px)" }}>
            {health ? (health.status === "healthy" ? `✓ Connected · ${health.product_count?.toLocaleString()} products` : "⚠ " + health.status) : "Check API Status"}
          </button>
        </div>
      </div>

      <div style={{ maxWidth: "1200px", margin: "0 auto", padding: "24px 20px" }}>
        <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: "24px", alignItems: "start" }}>

          {/* ── SIDEBAR ── */}
          <div style={{ background: "rgba(255,255,255,0.85)", backdropFilter: "blur(12px)", border: "1px solid #f0dcea", borderRadius: "24px", padding: "24px", position: "sticky", top: "20px", boxShadow: "0 8px 32px rgba(200,150,180,0.1)" }}>
            <h2 style={{ margin: "0 0 20px", fontSize: "22px", fontWeight: "800", color: "#4a2a4a" }}>Your Skin Profile ✨</h2>

            {/* Skin Type */}
            <div style={{ marginBottom: "16px" }}>
              <label style={{ display: "block", fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "6px" }}>Skin Type</label>
              <div style={{ display: "flex", gap: "6px", flexWrap: "wrap" }}>
                {SKIN_TYPES.map(t => (
                  <button key={t} onClick={() => setSkinType(t)} style={{
                    padding: "6px 12px", borderRadius: "20px", cursor: "pointer", fontSize: "12px",
                    fontFamily: "'Cormorant Garamond', Georgia, serif", fontWeight: "800",
                    border: skinType === t ? "none" : "1px solid #f0dcea",
                    background: skinType === t ? "linear-gradient(135deg, #e8a0b4, #c880a0)" : "white",
                    color: skinType === t ? "white" : "#9a7a88", transition: "all 0.2s",
                  }}>{t.charAt(0).toUpperCase() + t.slice(1)}</button>
                ))}
              </div>
            </div>

            {/* Acne Prone */}
            <div style={{ marginBottom: "16px" }}>
              <label style={{ display: "block", fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "6px" }}>Acne Prone?</label>
              <div style={{ display: "flex", gap: "8px" }}>
                {[true, false].map(v => (
                  <button key={String(v)} onClick={() => setAcneProne(v)} style={{
                    flex: 1, padding: "8px", borderRadius: "12px", cursor: "pointer", fontSize: "16px",
                    fontFamily: "'Cormorant Garamond', Georgia, serif", fontWeight: "800",
                    border: acneProne === v ? "none" : "1px solid #f0dcea",
                    background: acneProne === v ? "linear-gradient(135deg, #e8a0b4, #c880a0)" : "white",
                    color: acneProne === v ? "white" : "#9a7a88", transition: "all 0.2s",
                  }}>{v ? "Yes" : "No"}</button>
                ))}
              </div>
            </div>

            {/* ── Category Filter ── */}
            <div style={{ marginBottom: "12px" }}>
              <label style={{ display: "block", fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "6px" }}>
                 Category
              </label>
              <select value={selectedCategory} onChange={e => setSelectedCategory(e.target.value)} style={selectStyle}>
                {Object.keys(CATEGORY_MAP).map(cat => (
                  <option key={cat} value={cat}>{cat}</option>
                ))}
              </select>
            </div>

            {/* ── Sub-Category Filter — always visible with real subcategories ── */}
            <div style={{ marginBottom: "16px" }}>
              <label style={{ display: "block", fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "6px" }}>
                 Sub-Category
              </label>
              <select
                value={selectedSubCategory}
                onChange={e => setSelectedSubCategory(e.target.value)}
                style={{
                  ...selectStyle,
                  background: selectedSubCategory !== "All Subcategories" ? "linear-gradient(135deg, #fff0f8, #f8f0ff)" : "white",
                  border: selectedSubCategory !== "All Subcategories" ? "1px solid #e8a0b4" : "1px solid #f0dcea",
                }}
              >
                <option value="All Subcategories">— All Subcategories —</option>
                {(selectedCategory !== "All Categories"
                  ? (CATEGORY_MAP[selectedCategory] || []).filter(s => s !== "All Subcategories")
                  : ALL_SUBCATEGORIES
                ).map(sub => <option key={sub} value={sub}>{sub}</option>)}
              </select>
              {selectedSubCategory !== "All Subcategories" && (
                <div style={{ marginTop: "6px", display: "flex", alignItems: "center", gap: "6px" }}>
                  <span style={{ fontSize: "10px", background: "linear-gradient(135deg, #e8a0b4, #c880a0)", color: "white", padding: "3px 10px", borderRadius: "20px", fontWeight: "600" }}>
                    ✓ {selectedSubCategory}
                  </span>
                  <button onClick={() => setSelectedSubCategory("All Subcategories")} style={{ background: "none", border: "none", cursor: "pointer", fontSize: "13px", color: "#c090a8" }}>✕</button>
                </div>
              )}
            </div>


            {/* Budget */}
            <div style={{ marginBottom: "16px" }}>
              <label style={{ display: "block", fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "6px" }}>
                Max Budget: <span style={{ color: "#c880a0", fontWeight: "900" }}>${budget}</span>
              </label>
              <input type="range" min={5} max={500} step={5} value={budget} onChange={e => setBudget(Number(e.target.value))} style={{ width: "100%", accentColor: "#e8a0b4" }} />
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: "16px", color: "#c0a0b0", marginTop: "2px" }}>
                <span>$5</span><span>$500</span>
              </div>
            </div>

            {/* Results count */}
            <div style={{ marginBottom: "16px" }}>
              <label style={{ display: "block", fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "6px" }}>Results to Show</label>
              <select value={topN} onChange={e => setTopN(Number(e.target.value))} style={selectStyle}>
                {[3, 6, 9, 12, 15, 20].map(n => <option key={n} value={n}>{n} products</option>)}
              </select>
            </div>

            {/* Preferences */}
            <div style={{ marginBottom: "20px" }}>
              <label style={{ display: "block", fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "8px" }}>Preferences (optional)</label>
              <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
                {PREFERENCE_OPTIONS.map(({ key, label }) => (
                  <label key={key} style={{
                    display: "flex", alignItems: "center", gap: "8px", cursor: "pointer",
                    padding: "6px 10px", borderRadius: "10px",
                    background: preferences[key] ? "linear-gradient(135deg, #fce8f3, #f0e8ff)" : "transparent",
                    border: preferences[key] ? "1px solid #f0dcea" : "1px solid transparent",
                    transition: "all 0.15s",
                  }}>
                    <input type="checkbox" checked={!!preferences[key]} onChange={() => togglePref(key)} style={{ accentColor: "#e8a0b4", width: "14px", height: "14px" }} />
                    <span style={{ fontSize: "17px", color: preferences[key] ? "#7a3a6a" : "#9a7a88", fontWeight: preferences[key] ? "600" : "400" }}>{label}</span>
                  </label>
                ))}
              </div>
            </div>

            <button onClick={fetchRecommendations} disabled={loading} style={{
              width: "100%", padding: "14px",
              background: loading ? "#e0c8d4" : "linear-gradient(135deg, #e8a0b4 0%, #c880a0 50%, #a060a0 100%)",
              border: "none", borderRadius: "14px", color: "white", fontSize: "19px", fontWeight: "800",
              cursor: loading ? "not-allowed" : "pointer", fontFamily: "'Cormorant Garamond', Georgia, serif",
              letterSpacing: "0.05em", boxShadow: loading ? "none" : "0 6px 20px rgba(200,100,160,0.3)", transition: "all 0.2s",
            }}>
              {loading ? "Finding your glow... 🌸" : "Find My Products ✨"}
            </button>
          </div>

          {/* ── RESULTS ── */}
          <div>
            {/* Active filters bar */}
            {(selectedCategory !== "All Categories" || selectedSubCategory !== "All Subcategories") && (
              <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginBottom: "14px", alignItems: "center" }}>
                <span style={{ fontSize: "16px", color: "#b09aa8", textTransform: "uppercase", letterSpacing: "0.08em" }}>Filters:</span>
                {selectedCategory !== "All Categories" && (
                  <span style={{ background: "linear-gradient(135deg, #f7d6e8, #edd5f5)", color: "#7a3a6a", fontSize: "11px", padding: "4px 12px", borderRadius: "20px", fontWeight: "600", border: "1px solid #f0dcea" }}>
                    📦 {selectedCategory}
                  </span>
                )}
                {selectedSubCategory !== "All Subcategories" && selectedSubCategory !== "" && (
                  <span style={{ background: "linear-gradient(135deg, #e8a0b4, #c880a0)", color: "white", fontSize: "11px", padding: "4px 12px", borderRadius: "20px", fontWeight: "600" }}>
                    🔍 {selectedSubCategory}
                  </span>
                )}
                <button onClick={() => { setSelectedCategory("All Categories"); setSelectedSubCategory("All Subcategories"); }} style={{ background: "none", border: "1px solid #f0dcea", borderRadius: "20px", padding: "3px 10px", fontSize: "11px", color: "#c090a8", cursor: "pointer" }}>
                  Clear filters ✕
                </button>
              </div>
            )}

            {/* Results header */}
            {displayResults.length > 0 && (
              <div style={{ marginBottom: "16px", display: "flex", alignItems: "center", gap: "12px" }}>
                <h3 style={{ margin: 0, fontSize: "20px", color: "#4a2a4a", fontWeight: "800" }}>
                  {searchMode === "similar"
                    ? `✨ Similar to product #${similarProductId}`
                    : `✨ ${displayResults.length} products for ${skinType} skin`}
                </h3>
                {searchMode === "similar" && (
                  <button onClick={fetchRecommendations} style={{ background: "none", border: "1px solid #e8a0b4", borderRadius: "20px", padding: "4px 12px", fontSize: "11px", color: "#c880a0", cursor: "pointer", fontFamily: "'Cormorant Garamond', Georgia, serif" }}>← Back</button>
                )}
              </div>
            )}

            {/* Error */}
            {error && (
              <div style={{ background: "#fff0f0", border: "1px solid #ffc0c0", borderRadius: "14px", padding: "16px", marginBottom: "16px", fontSize: "13px", color: "#c05050" }}>
                ⚠️ {error}
                <div style={{ marginTop: "8px", fontSize: "16px", color: "#a07070" }}>Make sure your FastAPI server is running at {API_BASE}</div>
              </div>
            )}

            {/* Shimmer */}
            {loading && (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: "16px" }}>
                {Array(topN > 6 ? 6 : topN).fill(0).map((_, i) => (
                  <div key={i} style={{ background: "linear-gradient(90deg, #fce8f3 25%, #fff0f8 50%, #fce8f3 75%)", backgroundSize: "200% 100%", animation: "shimmer 1.5s infinite", borderRadius: "20px", height: "320px" }} />
                ))}
                <style>{`@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}`}</style>
              </div>
            )}

            {/* Grid */}
            {!loading && displayResults.length > 0 && (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: "16px" }}>
                {displayResults.map((p, i) => <ProductCard key={p.product_id || i} product={p} onSimilar={fetchSimilar} />)}
              </div>
            )}

            {/* No results after filter */}
            {!loading && results.length > 0 && displayResults.length === 0 && (
              <div style={{ textAlign: "center", padding: "60px 20px", color: "#c0a0b8" }}>
                <div style={{ fontSize: "50px", marginBottom: "12px" }}>🔍</div>
                <h3 style={{ margin: "0 0 8px", fontSize: "18px", fontWeight: "400", color: "#8a5a78" }}>No products in this sub-category</h3>
                <p style={{ margin: "0 0 16px", fontSize: "13px" }}>Try selecting "All Subcategories" or a different category</p>
                <button onClick={() => { setSelectedCategory("All Categories"); setSelectedSubCategory("All Subcategories"); }} style={{ background: "linear-gradient(135deg, #e8a0b4, #c880a0)", border: "none", borderRadius: "20px", padding: "8px 20px", color: "white", cursor: "pointer", fontSize: "12px", fontFamily: "'Cormorant Garamond', Georgia, serif" }}>
                  Clear Filters ✨
                </button>
              </div>
            )}

            {/* Empty state */}
            {!loading && results.length === 0 && !error && (
              <div style={{ textAlign: "center", padding: "80px 20px", color: "#c0a0b8" }}>
                <div style={{ fontSize: "64px", marginBottom: "16px" }}>🌸</div>
                <h3 style={{ margin: "0 0 8px", fontSize: "25px", fontWeight: "600", color: "#8a5a78" }}>Your perfect routine awaits</h3>
                <p style={{ margin: 0, fontSize: "13px", lineHeight: "1.6" }}>Choose your skin type, set your budget, and discover<br />products curated just for you.</p>
              </div>
            )}
          </div>
        </div>
      </div>

      <div style={{ textAlign: "center", padding: "24px", fontSize: "11px", color: "#c0a0b0", letterSpacing: "0.08em", textTransform: "uppercase" }}>
        GlowPipe Skin · Powered by Snowflake · {new Date().getFullYear()}
      </div>
    </div>
  );
}
