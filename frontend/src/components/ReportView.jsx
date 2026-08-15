import React, { useState } from 'react';

export default function ReportView({ job }) {
  const [copied, setCopied] = useState(false);

  if (!job || !job.report) {
    if (job && job.status !== 'completed') {
      return (
        <div className="glass-panel" style={{ padding: '32px', textAlign: 'center' }}>
          <div className="spinning-icon" style={{ fontSize: '2.5rem', marginBottom: '16px' }}>⚙️</div>
          <h3 style={{ fontSize: '1.2rem', fontWeight: 600, color: '#ffffff', marginBottom: '8px' }}>
            Assembling Research Report...
          </h3>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>
            The multi-agent pipeline is processing stage <strong style={{ color: 'var(--accent-cyan)' }}>{job.current_stage || job.status}</strong>.
          </p>
        </div>
      );
    }
    return null;
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(job.report);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const element = document.createElement('a');
    const file = new Blob([job.report], { type: 'text/markdown' });
    element.href = URL.createObjectURL(file);
    element.download = `Research-Report-${job.id.slice(0, 8)}.md`;
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  };

  return (
    <div className="glass-panel" style={{ padding: '36px', marginBottom: '40px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px', flexWrap: 'wrap', gap: '12px', borderBottom: '1px solid var(--border-color)', paddingBottom: '16px' }}>
        <div>
          <span className="badge badge-completed" style={{ marginBottom: '8px' }}>
            ✓ Verified Final Report
          </span>
          <h2 style={{ fontSize: '1.5rem', fontWeight: 800, color: '#ffffff' }}>
            {job.topic}
          </h2>
        </div>

        <div style={{ display: 'flex', gap: '10px' }}>
          <button
            type="button"
            onClick={handleCopy}
            style={{
              background: 'rgba(255, 255, 255, 0.08)',
              border: '1px solid var(--border-color)',
              color: '#ffffff',
              padding: '8px 16px',
              borderRadius: '8px',
              cursor: 'pointer',
              fontWeight: 600,
              fontSize: '0.85rem',
            }}
          >
            {copied ? '✓ Copied!' : '📋 Copy Markdown'}
          </button>
          <button
            type="button"
            onClick={handleDownload}
            className="btn-primary"
            style={{ padding: '8px 18px', fontSize: '0.85rem' }}
          >
            📥 Download .md
          </button>
        </div>
      </div>

      {/* Render Markdown text in styled block */}
      <article
        style={{
          lineHeight: 1.8,
          fontSize: '0.95rem',
          color: '#e5e7eb',
          whiteSpace: 'pre-wrap',
          fontFamily: 'var(--font-sans)',
        }}
      >
        {job.report}
      </article>
    </div>
  );
}

