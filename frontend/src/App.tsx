import React, { useEffect, useState } from 'react';

type GlobalMetric = {
  id: number;
  name: string;
  value: number;
  window: string;
  calculated_at: string;
};

type RealtimeMetrics = {
  free_agents: number;
  in_call: number;
  in_queue: number;
};

const API_BASE = 'http://localhost:8081';
const WS_URL = 'ws://localhost:8080/ws/realtime';

export const App: React.FC = () => {
  const [globalMetrics, setGlobalMetrics] = useState<GlobalMetric[]>([]);
  const [realtime, setRealtime] = useState<RealtimeMetrics | null>(null);
  const [wsStatus, setWsStatus] = useState<'connecting' | 'open' | 'closed'>('connecting');

  // загрузка агрегированных метрик
  useEffect(() => {
    fetch(`${API_BASE}/api/metrics/global`)
      .then((r) => r.json())
      .then((data: GlobalMetric[]) => setGlobalMetrics(data))
      .catch((err) => {
        console.error('Failed to load global metrics', err);
      });
  }, []);

  // подписка на WebSocket для realtime-метрик
  useEffect(() => {
    const ws = new WebSocket(WS_URL);
    setWsStatus('connecting');

    ws.onopen = () => setWsStatus('open');

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as RealtimeMetrics;
        setRealtime(data);
      } catch (e) {
        console.error('Failed to parse realtime metrics', e);
      }
    };

    ws.onclose = () => setWsStatus('closed');
    ws.onerror = () => setWsStatus('closed');

    return () => ws.close();
  }, []);

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', padding: '24px', maxWidth: 960, margin: '0 auto' }}>
      <h1>Contact Center Monitoring</h1>
      <p>Простое демо-приложение для диплома: realtime и агрегированные метрики контакт-центра.</p>

      <section style={{ marginTop: 24 }}>
        <h2>Realtime-метрики (WebSocket)</h2>
        <p>WebSocket статус: {wsStatus}</p>

        {realtime ? (
          <div
            style={{
              display: 'flex',
              gap: 16,
              marginTop: 12,
            }}
          >
            <Card label="Свободные операторы" value={realtime.free_agents} />
            <Card label="Активные звонки" value={realtime.in_call} />
            <Card label="В очереди" value={realtime.in_queue} />
          </div>
        ) : (
          <p>Ожидание данных по WebSocket...</p>
        )}
      </section>

      <section style={{ marginTop: 32 }}>
        <h2>Глобальные метрики (PostgreSQL)</h2>
        {globalMetrics.length === 0 ? (
          <p>Пока нет агрегированных данных. Сервис analyser добавит записи в global_metrics.</p>
        ) : (
          <table
            style={{
              width: '100%',
              borderCollapse: 'collapse',
              marginTop: 12,
            }}
          >
            <thead>
              <tr>
                <th style={thStyle}>Имя</th>
                <th style={thStyle}>Значение</th>
                <th style={thStyle}>Окно</th>
                <th style={thStyle}>Время расчёта</th>
              </tr>
            </thead>
            <tbody>
              {globalMetrics.map((m) => (
                <tr key={m.id}>
                  <td style={tdStyle}>{m.name}</td>
                  <td style={tdStyle}>{m.value.toFixed(2)}</td>
                  <td style={tdStyle}>{m.window}</td>
                  <td style={tdStyle}>{new Date(m.calculated_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
};

const Card: React.FC<{ label: string; value: number }> = ({ label, value }) => (
  <div
    style={{
      flex: 1,
      padding: 16,
      borderRadius: 8,
      border: '1px solid #e0e0e0',
      boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
      backgroundColor: '#fafafa',
    }}
  >
    <div style={{ fontSize: 14, color: '#555' }}>{label}</div>
    <div style={{ fontSize: 28, fontWeight: 600, marginTop: 8 }}>{value}</div>
  </div>
);

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '8px 12px',
  borderBottom: '1px solid #e0e0e0',
  backgroundColor: '#fafafa',
};

const tdStyle: React.CSSProperties = {
  padding: '8px 12px',
  borderBottom: '1px solid #f0f0f0',
};

