import React, { useState } from 'react';

const SUGGESTIONS = [
  'AI Agents in Enterprise Architecture (Deep Redo Demo)',
  'Quantum Computing Breakthroughs in 2026',
  'Next-Gen Solid State Battery Commercialization',
  'Autonomous Electric Aviation & Urban Air Mobility',
];

export default function TopicForm({ onSubmit, loading }) {
  const [topic, setTopic] = useState('');

  const handleSubmit = (event) => {
    event.preventDefault();
    if (topic.trim()) {
      onSubmit(topic.trim());
    }
  };

  const handleChipClick = (suggestion) => {
    setTopic(suggestion);
    onSubmit(suggestion);
  };

  return (
    <div className="glass-panel" style={{ padding: '28px', marginBottom: '32px' }}>
      <div style={{ marginBottom: '16px' }}>
        <h2 style={{ fontSize: '1.25rem', fontWeight: 700, color: '#ffffff', marginBottom: '6px' }}>
          🚀 Initiate Research Pipeline
        </h2>
        <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>
          Enter any complex topic. The multi-agent cluster will plan, search, summarize, fact-check, and compile a structured report.
        </p>
      </div>

      <form onSubmit={handleSubmit} style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', marginBottom: '20px' }}>
        <input
          type="text"
          className="input-field"
          value={topic}
          onChange={(e) => setTopic(e.target.value)}
          placeholder="e.g. AI Agents in Enterprise Architecture (deep research)..."
          style={{ flex: '1 1 400px' }}
          disabled={loading}
        />
        <button type="submit" className="btn-primary" disabled={loading || !topic.trim()}>
          {loading ? (
            <>
              <span className="spinning-icon">⚙️</span> Orchestrating Agents...
            </>
          ) : (
            <>
              ⚡ Generate Report
            </>
          )}
        </button>
      </form>

      <div>
        <span style={{ fontSize: '0.8rem', color: 'var(--text-dim)', marginRight: '10px', fontWeight: 600 }}>
          SUGGESTED TOPICS:
        </span>
        <div style={{ display: 'inline-flex', gap: '8px', flexWrap: 'wrap', marginTop: '6px' }}>
          {SUGGESTIONS.map((sug) => (
            <button
              key={sug}
              type="button"
              onClick={() => handleChipClick(sug)}
              disabled={loading}
              style={{
                background: 'rgba(255, 255, 255, 0.05)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '20px',
                color: '#d1d5db',
                padding: '5px 14px',
                fontSize: '0.8rem',
                cursor: loading ? 'not-allowed' : 'pointer',
                transition: 'all 0.2s ease',
              }}
              onMouseEnter={(e) => {
                e.target.style.background = 'rgba(99, 102, 241, 0.2)';
                e.target.style.borderColor = 'var(--primary)';
              }}
              onMouseLeave={(e) => {
                e.target.style.background = 'rgba(255, 255, 255, 0.05)';
                e.target.style.borderColor = 'rgba(255, 255, 255, 0.1)';
              }}
            >
              {sug}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

