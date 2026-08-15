import React from 'react';

const STAGES = [
  { id: 'planning', label: '1. Planner Agent', topic: 'research.requested → research.planned', desc: 'Deconstructs topic into research sub-questions & outline' },
  { id: 'searching', label: '2. Searcher Agent', topic: 'research.planned → research.searched', desc: 'Gathers live web search sources and technical citations' },
  { id: 'summarizing', label: '3. Summarizer Agent', topic: 'research.searched → research.summarized', desc: 'Synthesizes sources into structured section notes' },
  { id: 'critiquing', label: '4. Critic Agent', topic: 'research.summarized → research.critiqued', desc: 'Fact-checks claims & evaluates depth (Triggers Redo Loop if weak)' },
  { id: 'completed', label: '5. Compiler Agent', topic: 'research.critiqued → research.completed', desc: 'Merges verified sections into final formatted report' },
];

const getStageIndex = (stage) => {
  switch (stage) {
    case 'planning':
    case 'requested':
      return 0;
    case 'planned':
    case 'searching':
      return 1;
    case 'searched':
    case 'summarizing':
      return 2;
    case 'summarized':
    case 'critiquing':
      return 3;
    case 'critiqued':
    case 'compiling':
    case 'completed':
      return 4;
    default:
      return 0;
  }
};

export default function PipelineTracker({ job, logs = [] }) {
  if (!job) return null;

  const currentStageStr = job.current_stage || job.status || 'planning';
  const currentIndex = getStageIndex(currentStageStr);
  const isCompleted = job.status === 'completed';
  const retryCount = job.retry_count || 0;

  return (
    <div className="glass-panel" style={{ padding: '28px', marginBottom: '32px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px', flexWrap: 'wrap', gap: '12px' }}>
        <div>
          <h2 style={{ fontSize: '1.25rem', fontWeight: 700, color: '#ffffff', display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span>⚡ Live Agent Pipeline Tracker</span>
            {retryCount > 0 ? (
              <span className="badge badge-redo">
                🔁 Redo Loop (Pass {retryCount + 1})
              </span>
            ) : null}
          </h2>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>
            Topic: <strong style={{ color: 'var(--text-main)' }}>"{job.topic}"</strong> | Job ID: <code style={{ fontFamily: 'var(--font-mono)', fontSize: '0.8rem', color: 'var(--accent-cyan)' }}>{job.id?.slice(0, 8)}</code>
          </p>
        </div>

        <div>
          <span className={`badge badge-${job.status === 'completed' ? 'completed' : currentStageStr}`}>
            ● {isCompleted ? 'Pipeline Completed' : `Active Stage: ${currentStageStr}`}
          </span>
        </div>
      </div>

      {/* Visual Pipeline Grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '16px', marginBottom: '28px' }}>
        {STAGES.map((s, idx) => {
          const isDone = isCompleted || idx < currentIndex;
          const isActive = !isCompleted && idx === currentIndex;
          const isPending = idx > currentIndex && !isCompleted;

          let statusBg = 'rgba(255, 255, 255, 0.03)';
          let borderColor = 'rgba(255, 255, 255, 0.08)';
          let icon = '⚪';

          if (isDone) {
            statusBg = 'rgba(16, 185, 129, 0.08)';
            borderColor = 'rgba(16, 185, 129, 0.3)';
            icon = '✅';
          } else if (isActive) {
            statusBg = 'rgba(99, 102, 241, 0.15)';
            borderColor = 'var(--primary)';
            icon = '⚙️';
          }

          return (
            <div
              key={s.id}
              className={isActive ? 'pulsing-node' : ''}
              style={{
                background: statusBg,
                border: `1px solid ${borderColor}`,
                borderRadius: '12px',
                padding: '16px',
                transition: 'all 0.3s ease',
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                <span style={{ fontSize: '0.85rem', fontWeight: 700, color: isActive ? '#ffffff' : isDone ? '#34d399' : 'var(--text-muted)' }}>
                  {s.label}
                </span>
                <span style={{ fontSize: '1rem' }}>{icon}</span>
              </div>
              <p style={{ fontSize: '0.75rem', color: 'var(--text-dim)', lineHeight: 1.4 }}>
                {s.desc}
              </p>
              <div style={{ marginTop: '8px', fontSize: '0.65rem', fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)', opacity: 0.8 }}>
                {s.topic}
              </div>
            </div>
          );
        })}
      </div>

      {/* Live Stage Logs Console */}
      {logs && logs.length > 0 ? (
        <div style={{ background: 'rgba(11, 15, 25, 0.9)', border: '1px solid rgba(255, 255, 255, 0.1)', borderRadius: '10px', padding: '16px' }}>
          <h4 style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginBottom: '10px', fontFamily: 'var(--font-mono)' }}>
            📡 Event Stream & Agent Activity Logs ({logs.length} events)
          </h4>
          <div style={{ maxHeight: '140px', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {logs.map((log, idx) => (
              <div key={idx} style={{ fontSize: '0.78rem', fontFamily: 'var(--font-mono)', color: '#d1d5db', display: 'flex', gap: '10px' }}>
                <span style={{ color: 'var(--text-dim)' }}>[{new Date(log.timestamp * 1000).toLocaleTimeString()}]</span>
                <span style={{ color: 'var(--accent-cyan)', fontWeight: 600 }}>[{log.stage.toUpperCase()}]</span>
                <span style={{ color: '#9ca3af' }}>Published payload to topic: {log.stage}</span>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

