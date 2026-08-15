import React, { useEffect, useRef, useState } from 'react';
import TopicForm from './components/TopicForm';
import PipelineTracker from './components/PipelineTracker';
import ReportView from './components/ReportView';

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000').replace(/\/$/, '');
const WS_BASE_URL = API_BASE_URL.replace(/^http/, 'ws');

export default function App() {
  const [job, setJob] = useState(null);
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const wsRef = useRef(null);

  // Poll fallback & detail fetcher
  const fetchJobDetails = async (jobId) => {
    try {
      const response = await fetch(`${API_BASE_URL}/research/${jobId}`);
      if (response.ok) {
        const data = await response.json();
        setJob(data);
        if (data.logs) {
          setLogs(data.logs);
        }
        return data;
      }
    } catch (e) {
      console.warn('Job details fetch warning:', e);
    }
    return null;
  };

  // WebSocket lifecycle
  useEffect(() => {
    if (!job || job.status === 'completed' || job.status === 'failed') {
      return undefined;
    }

    const wsUrl = `${WS_BASE_URL}/ws/${job.id}`;
    const socket = new WebSocket(wsUrl);
    wsRef.current = socket;

    socket.onopen = () => {
      console.log('WebSocket connected for job:', job.id);
    };

    socket.onmessage = (event) => {
      try {
        const update = JSON.parse(event.data);
        console.log('WebSocket event received:', update);
        setJob((prev) => ({
          ...prev,
          status: update.status || update.stage || prev?.status,
          current_stage: update.stage || prev?.current_stage,
          retry_count: update.retry_count !== undefined ? update.retry_count : prev?.retry_count,
          report: update.payload?.final_report || update.report || prev?.report,
        }));

        fetchJobDetails(job.id);
      } catch (err) {
        console.error('Error parsing WebSocket update:', err);
      }
    };

    socket.onerror = (err) => {
      console.warn('WebSocket error, using HTTP polling fallback', err);
    };

    socket.onclose = () => {
      console.log('WebSocket closed');
    };

    // Polling fallback interval every 1s
    const intervalId = window.setInterval(async () => {
      const updated = await fetchJobDetails(job.id);
      if (updated && (updated.status === 'completed' || updated.status === 'failed')) {
        window.clearInterval(intervalId);
      }
    }, 1000);

    return () => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.close();
      }
      window.clearInterval(intervalId);
    };
  }, [job?.id, job?.status]);

  const handleSubmit = async (topic) => {
    setLoading(true);
    setError('');
    setJob(null);
    setLogs([]);

    try {
      const response = await fetch(`${API_BASE_URL}/research`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic }),
      });

      if (!response.ok) {
        const errPayload = await response.json().catch(() => null);
        throw new Error(errPayload?.detail || 'Failed to submit research topic');
      }

      const createdJob = await response.json();
      setJob(createdJob);
      fetchJobDetails(createdJob.id);
    } catch (err) {
      setError(err.message || 'An error occurred submitting topic.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ minHeight: '100vh', padding: '40px 20px 80px 20px' }}>
      <header style={{ maxWidth: '1080px', margin: '0 auto 36px auto', textAlign: 'center' }}>
        <div style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', padding: '6px 16px', background: 'rgba(99, 102, 241, 0.15)', border: '1px solid rgba(99, 102, 241, 0.3)', borderRadius: '30px', color: 'var(--accent-cyan)', fontSize: '0.8rem', fontWeight: 700, marginBottom: '16px', letterSpacing: '0.05em' }}>
          <span>⚡ KAFKA EVENT-DRIVEN AI PIPELINE</span>
        </div>
        <h1 style={{ fontSize: '2.8rem', fontWeight: 800, color: '#ffffff', letterSpacing: '-0.02em', marginBottom: '12px' }}>
          StreamAgent AI
        </h1>
        <p style={{ color: 'var(--text-muted)', fontSize: '1.05rem', maxWidth: '720px', margin: '0 auto' }}>
          Real-Time Multi-Agent Deep Research Generator · Redpanda Kafka · FastAPI · React · WebSockets
        </p>
      </header>

      <main style={{ maxWidth: '1080px', margin: '0 auto' }}>
        <TopicForm onSubmit={handleSubmit} loading={loading} />

        {error ? (
          <div className="glass-panel" style={{ padding: '16px 20px', marginBottom: '24px', borderColor: 'rgba(239, 68, 68, 0.4)', background: 'rgba(239, 68, 68, 0.1)', color: '#f87171' }}>
            ⚠️ {error}
          </div>
        ) : null}

        <PipelineTracker job={job} logs={logs} />
        <ReportView job={job} />
      </main>

      <footer style={{ maxWidth: '1080px', margin: '40px auto 0 auto', textAlign: 'center', color: 'var(--text-dim)', fontSize: '0.85rem' }}>
        Placement Portfolio Project — Full-stack Event-Driven Multi-Agent Architecture
      </footer>
    </div>
  );
}

