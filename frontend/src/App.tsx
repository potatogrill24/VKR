import { useEffect, useState, useCallback } from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  Legend,
} from 'recharts';

const API_BASE = 'http://localhost:8081';
const WS_URL = 'ws://localhost:8081/ws/realtime';

type RealtimeMetrics = {
  agents_available: number;
  agents_in_call: number;
  agents_wrap_up: number;
  agents_offline: number;
  calls_in_queue: number;
  longest_wait_sec: number;
  avg_wait_sec: number;
  calls_per_minute: number;
  service_level: number;
  abandonment_rate: number;
  updated_at: string;
};

type LatestMetrics = {
  [window: string]: {
    calls_count?: number;
    avg_wait_seconds?: number;
    avg_talk_seconds?: number;
    avg_handle_time?: number;
    sla_percent?: number;
    abandonment_rate?: number;
    transfer_rate?: number;
    fcr_rate?: number;
    avg_customer_rating?: number;
    avg_sentiment?: number;
    calculated_at?: string;
  };
};

type QueueMetricsByWindow = {
  [window: string]: {
    [queue: string]: {
      calls_count: number;
      avg_wait_seconds: number;
      sla_percent: number;
      abandonment_rate: number;
    };
  };
};

type Agent = {
  agent_id: string;
  full_name: string;
  email: string;
  primary_queue: string;
  skills: string[];
  hire_date: string;
  is_active: boolean;
};

type StatusData = {
  status: string;
  count: number;
};

type AgentStats = {
  agent_id: string;
  full_name: string;
  calls_count: number;
  avg_talk_time: number;
  sla_percent: number;
  avg_rating: number;
};

export const App = () => {
  const [realtime, setRealtime] = useState<RealtimeMetrics | null>(null);
  const [realtimeHistory, setRealtimeHistory] = useState<RealtimeMetrics[]>([]);
  const [latestMetrics, setLatestMetrics] = useState<LatestMetrics>({});
  const [queueMetrics, setQueueMetrics] = useState<QueueMetricsByWindow>({});
  const [agents, setAgents] = useState<Agent[]>([]);
  const [wsStatus, setWsStatus] = useState<'connecting' | 'open' | 'closed' | 'error'>('connecting');
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [activeTab, setActiveTab] = useState<'realtime' | 'analytics' | 'queues' | 'agents'>('realtime');

  const [statusDistribution, setStatusDistribution] = useState<StatusData[]>([]);
  const [topAgents, setTopAgents] = useState<AgentStats[]>([]);

  const fetchLatestMetrics = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/metrics/latest`);
      const data = await res.json();
      setLatestMetrics(data);
    } catch (err) {
      console.error('Ошибка загрузки метрик', err);
    }
  }, []);

  const fetchQueueMetrics = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/metrics/queues`);
      const data = await res.json();
      setQueueMetrics(data);
    } catch (err) {
      console.error('Ошибка загрузки метрик очередей', err);
    }
  }, []);

  const fetchAgents = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/agents`);
      const data = await res.json();
      setAgents(data || []);
    } catch (err) {
      console.error('Ошибка загрузки операторов', err);
    }
  }, []);

  const fetchAnalytics = useCallback(async () => {
    try {
      const [statusRes, agentsRes] = await Promise.all([
        fetch(`${API_BASE}/api/stats/status-distribution`),
        fetch(`${API_BASE}/api/stats/top-agents`),
      ]);

      const [status, agents] = await Promise.all([
        statusRes.json(),
        agentsRes.json(),
      ]);

      setStatusDistribution(status || []);
      setTopAgents(agents || []);
    } catch (err) {
      console.error('Ошибка загрузки аналитики', err);
    }
  }, []);

  useEffect(() => {
    // Загружаем все данные одновременно для консистентности
    const fetchAll = async () => {
      await Promise.all([
        fetchLatestMetrics(),
        fetchQueueMetrics(),
        fetchAnalytics(),
      ]);
    };

    fetchAgents();
    fetchAll();

    const interval = setInterval(fetchAll, 120000); // Обновление каждые 2 минуты

    return () => clearInterval(interval);
  }, [fetchLatestMetrics, fetchQueueMetrics, fetchAgents, fetchAnalytics]);

  useEffect(() => {
    let ws: WebSocket;
    let reconnectTimeout: ReturnType<typeof setTimeout>;

    const connect = () => {
      ws = new WebSocket(WS_URL);
      setWsStatus('connecting');

      ws.onopen = () => {
        setWsStatus('open');
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data) as RealtimeMetrics;
          setRealtime(data);
          setLastUpdate(new Date());
          setRealtimeHistory((prev) => {
            const newHistory = [...prev, data].slice(-60);
            return newHistory;
          });
        } catch (e) {
          console.error('Ошибка парсинга данных', e);
        }
      };

      ws.onclose = () => {
        setWsStatus('closed');
        reconnectTimeout = setTimeout(connect, 3000);
      };

      ws.onerror = () => {
        setWsStatus('error');
      };
    };

    connect();

    return () => {
      clearTimeout(reconnectTimeout);
      ws?.close();
    };
  }, []);

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <div style={styles.headerContent}>
          <h1 style={styles.title}>Мониторинг контакт-центра</h1>
          <div style={styles.headerRight}>
            <ConnectionStatus status={wsStatus} />
            {lastUpdate && (
              <span style={styles.lastUpdate}>
                Обновлено: {lastUpdate.toLocaleTimeString()}
              </span>
            )}
          </div>
        </div>
      </header>

      <nav style={styles.nav}>
        <TabButton active={activeTab === 'realtime'} onClick={() => setActiveTab('realtime')}>
          В реальном времени
        </TabButton>
        <TabButton active={activeTab === 'analytics'} onClick={() => setActiveTab('analytics')}>
          По звонкам
        </TabButton>
        <TabButton active={activeTab === 'queues'} onClick={() => setActiveTab('queues')}>
          По очередям
        </TabButton>
        <TabButton active={activeTab === 'agents'} onClick={() => setActiveTab('agents')}>
          Список операторов
        </TabButton>
      </nav>

      <main style={styles.main}>
        {activeTab === 'realtime' && (
          <RealtimeTab realtime={realtime} history={realtimeHistory} />
        )}
        {activeTab === 'analytics' && (
          <AnalyticsTab
            latestMetrics={latestMetrics}
            statusDistribution={statusDistribution}
            topAgents={topAgents}
          />
        )}
        {activeTab === 'queues' && <QueuesTab metrics={queueMetrics} />}
        {activeTab === 'agents' && <AgentsTab agents={agents} />}
      </main>
    </div>
  );
};

const ConnectionStatus = ({ status }: { status: string }) => {
  const colors: Record<string, string> = {
    connecting: '#f59e0b',
    open: '#10b981',
    closed: '#ef4444',
    error: '#ef4444',
  };
  const labels: Record<string, string> = {
    connecting: 'Подключение...',
    open: 'Подключено',
    closed: 'Отключено',
    error: 'Ошибка',
  };

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <div
        style={{
          width: 10,
          height: 10,
          borderRadius: '50%',
          backgroundColor: colors[status],
          animation: status === 'open' ? 'pulse 2s infinite' : undefined,
        }}
      />
      <span style={{ fontSize: 14, color: '#94a3b8' }}>{labels[status]}</span>
    </div>
  );
};

const TabButton = ({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) => (
  <button
    onClick={onClick}
    style={{
      ...styles.tabButton,
      backgroundColor: active ? '#3b82f6' : 'transparent',
      color: active ? '#fff' : '#64748b',
    }}
  >
    {children}
  </button>
);

const RealtimeTab = ({
  realtime,
  history,
}: {
  realtime: RealtimeMetrics | null;
  history: RealtimeMetrics[];
}) => {
  if (!realtime) {
    return (
      <div style={styles.emptyState}>
        <p>Ожидание данных...</p>
        <p style={{ fontSize: 14, color: '#94a3b8' }}>
          Убедитесь, что сервисы запущены и генератор отправляет события
        </p>
      </div>
    );
  }

  const chartData = history.map((h, i) => ({
    time: i,
    sl: h.service_level,
    abandon: h.abandonment_rate,
  }));

  return (
    <div>
      <h2 style={styles.sectionTitle}>Состояние операторов</h2>
      <div style={styles.cardGrid}>
        <MetricCard
          label="Свободны"
          value={realtime.agents_available}
          color="#10b981"
          icon="👤"
        />
        <MetricCard
          label="На линии"
          value={realtime.agents_in_call}
          color="#3b82f6"
          icon="📞"
        />
        <MetricCard
          label="Обработка"
          value={realtime.agents_wrap_up}
          color="#f59e0b"
          icon="📝"
        />
      </div>

      <h2 style={styles.sectionTitle}>Качество обслуживания</h2>
      <div style={styles.cardGrid}>
        <MetricCard
          label="Уровень сервиса (SL)"
          value={realtime.service_level}
          suffix="%"
          color={realtime.service_level >= 80 ? '#10b981' : realtime.service_level >= 60 ? '#f59e0b' : '#ef4444'}
          icon="🎯"
          decimals={1}
        />
        <MetricCard
          label="Потерянные звонки"
          value={realtime.abandonment_rate}
          suffix="%"
          color={realtime.abandonment_rate <= 5 ? '#10b981' : realtime.abandonment_rate <= 10 ? '#f59e0b' : '#ef4444'}
          icon="📉"
          decimals={1}
        />
        <MetricCard
          label="В очереди"
          value={realtime.calls_in_queue}
          color="#8b5cf6"
          icon="📋"
        />
        <MetricCard
          label="Среднее ожидание"
          value={realtime.avg_wait_sec}
          suffix=" сек"
          color={realtime.avg_wait_sec > 20 ? '#f59e0b' : '#10b981'}
          icon="⏳"
        />
      </div>

      {chartData.length > 5 && (
        <>
          <h2 style={styles.sectionTitle}>Динамика метрик (последние 60 сек)</h2>
          <div style={styles.chartContainer}>
            <ResponsiveContainer width="100%" height={250}>
              <AreaChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="time" tick={{ fontSize: 12 }} stroke="#94a3b8" />
                <YAxis tick={{ fontSize: 12 }} stroke="#94a3b8" domain={[0, 100]} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#fff', border: '1px solid #e2e8f0', borderRadius: 8 }}
                  formatter={(value: number, name: string) => {
                    const labels: Record<string, string> = {
                      sl: 'Уровень сервиса',
                      abandon: 'Потери',
                    };
                    return [value.toFixed(1) + '%', labels[name] || name];
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="sl"
                  stroke="#10b981"
                  fill="#10b98133"
                  strokeWidth={2}
                  name="sl"
                />
                <Area
                  type="monotone"
                  dataKey="abandon"
                  stroke="#ef4444"
                  fill="#ef444433"
                  strokeWidth={2}
                  name="abandon"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
    </div>
  );
};

const AnalyticsTab = ({
  latestMetrics,
  statusDistribution,
  topAgents,
}: {
  latestMetrics: LatestMetrics;
  statusDistribution: StatusData[];
  topAgents: AgentStats[];
}) => {
  const statusLabels: Record<string, string> = {
    completed: 'Завершён',
    abandoned: 'Потерян',
    transferred: 'Переведён',
    voicemail: 'Голосовая почта',
  };

  const statusColors: Record<string, string> = {
    completed: '#10b981',
    abandoned: '#ef4444',
    transferred: '#f59e0b',
    voicemail: '#8b5cf6',
  };

  const pieData = statusDistribution.map((s) => ({
    name: statusLabels[s.status] || s.status,
    value: s.count,
    color: statusColors[s.status] || '#94a3b8',
  }));

  return (
    <div>
      <h2 style={styles.sectionTitle}>Глобальные метрики</h2>
      <GlobalMetricsTable metrics={latestMetrics} />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginTop: 32 }}>
        <div>
          <h2 style={styles.sectionTitle}>Распределение статусов (за 10 мин)</h2>
          <div style={styles.chartContainer}>
            {pieData.length > 0 ? (
              <ResponsiveContainer width="100%" height={280}>
                <PieChart>
                  <Pie
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={100}
                    paddingAngle={2}
                    dataKey="value"
                    label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                    labelLine={false}
                  >
                    {pieData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div style={styles.emptyChart}>Нет данных</div>
            )}
          </div>
        </div>

        <div>
          <h2 style={styles.sectionTitle}>Топ операторов (за 10 мин)</h2>
          <div style={styles.chartContainer}>
            {topAgents.length > 0 ? (
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={topAgents} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis type="number" tick={{ fontSize: 12 }} stroke="#94a3b8" />
                  <YAxis
                    type="category"
                    dataKey="full_name"
                    tick={{ fontSize: 11 }}
                    stroke="#94a3b8"
                    width={110}
                  />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#fff', border: '1px solid #e2e8f0', borderRadius: 8 }}
                    formatter={(value: number) => [value, 'Звонков']}
                  />
                  <Bar dataKey="calls_count" fill="#3b82f6" name="Звонков" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div style={styles.emptyChart}>Нет данных</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

const GlobalMetricsTable = ({ metrics }: { metrics: LatestMetrics }) => {
  const windows = ['2m', '10m'];
  const windowLabels: Record<string, string> = { '2m': 'За 2 минуты', '10m': 'За 10 минут' };
  const metricNames: Record<string, string> = {
    calls_count: 'Количество звонков',
    avg_wait_seconds: 'Среднее ожидание',
    avg_talk_seconds: 'Среднее время разговора',
    avg_handle_time: 'Среднее время обработки (AHT)',
    sla_percent: 'Уровень сервиса (SL)',
    abandonment_rate: 'Потерянные звонки',
    transfer_rate: 'Переведённые звонки',
    fcr_rate: 'Решено с первого раза (FCR)',
    avg_customer_rating: 'Средняя оценка клиентов',
    avg_sentiment: 'Тональность разговоров',
  };

  const hasData = Object.keys(metrics).length > 0;
  if (!hasData) {
    return (
      <div style={styles.emptyChart}>
        Данные появятся после накопления истории звонков (обновление каждые 2 минуты)
      </div>
    );
  }

  return (
    <div style={styles.tableContainer}>
      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>Метрика</th>
            {windows.map((w) => (
              <th key={w} style={styles.th}>{windowLabels[w]}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Object.entries(metricNames).map(([key, label]) => (
            <tr key={key}>
              <td style={styles.td}>{label}</td>
              {windows.map((w) => {
                const value = metrics[w]?.[key as keyof typeof metrics[typeof w]];
                return (
                  <td key={w} style={styles.tdValue}>
                    {value !== undefined ? formatValue(key, value as number) : '—'}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {metrics['2m']?.calculated_at && (
        <p style={styles.footnote}>
          Последний расчёт: {new Date(metrics['2m'].calculated_at).toLocaleString()}
        </p>
      )}
    </div>
  );
};

const QueuesTab = ({ metrics }: { metrics: QueueMetricsByWindow }) => {
  const queueNames: Record<string, string> = {
    support: 'Поддержка',
    sales: 'Продажи',
    billing: 'Биллинг',
    'tech-support': 'Тех. поддержка',
    vip: 'VIP',
  };

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

  const windowData2m = metrics['2m'] || {};
  const windowData10m = metrics['10m'] || {};
  const queues2m = Object.keys(windowData2m);
  const queues10m = Object.keys(windowData10m);

  const chartData2m = queues2m.map((q) => ({
    name: queueNames[q] || q,
    calls: Math.round(windowData2m[q]?.calls_count || 0),
  }));

  const chartData10m = queues10m.map((q) => ({
    name: queueNames[q] || q,
    calls: Math.round(windowData10m[q]?.calls_count || 0),
  }));

  const total2m = chartData2m.reduce((sum, d) => sum + d.calls, 0);
  const total10m = chartData10m.reduce((sum, d) => sum + d.calls, 0);

  return (
    <div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        <div>
          <h2 style={styles.sectionTitle}>Распределение по очередям (за 2 мин)</h2>
          <p style={styles.hint}>Всего звонков: {total2m}</p>
          {chartData2m.length > 0 ? (
            <div style={styles.chartContainer}>
              <ResponsiveContainer width="100%" height={280}>
                <PieChart>
                  <Pie
                    data={chartData2m}
                    cx="50%"
                    cy="50%"
                    innerRadius={50}
                    outerRadius={90}
                    paddingAngle={2}
                    dataKey="calls"
                    label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                    labelLine={false}
                  >
                    {chartData2m.map((_, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: number) => [value, 'Звонков']} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={styles.emptyChart}>Нет данных</div>
          )}
        </div>

        <div>
          <h2 style={styles.sectionTitle}>Распределение по очередям (за 10 мин)</h2>
          <p style={styles.hint}>Всего звонков: {total10m}</p>
          {chartData10m.length > 0 ? (
            <div style={styles.chartContainer}>
              <ResponsiveContainer width="100%" height={280}>
                <PieChart>
                  <Pie
                    data={chartData10m}
                    cx="50%"
                    cy="50%"
                    innerRadius={50}
                    outerRadius={90}
                    paddingAngle={2}
                    dataKey="calls"
                    label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                    labelLine={false}
                  >
                    {chartData10m.map((_, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: number) => [value, 'Звонков']} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div style={styles.emptyChart}>Нет данных</div>
          )}
        </div>
      </div>

      <h2 style={styles.sectionTitle}>Детали по очередям (за 2 минуты)</h2>
      {queues2m.length > 0 ? (
        <div style={styles.queueGrid}>
          {queues2m.map((queue) => {
            const m = windowData2m[queue];
            if (!m) return null;
            return (
              <div key={queue} style={styles.queueCard}>
                <h3 style={styles.queueTitle}>{queueNames[queue] || queue}</h3>
                <div style={styles.queueMetrics}>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Звонков</span>
                    <span style={styles.queueMetricValue}>{Math.round(m.calls_count || 0)}</span>
                  </div>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Ср. ожидание</span>
                    <span style={styles.queueMetricValue}>{Math.round(m.avg_wait_seconds || 0)} сек</span>
                  </div>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Уровень сервиса</span>
                    <span
                      style={{
                        ...styles.queueMetricValue,
                        color: (m.sla_percent || 0) >= 80 ? '#10b981' : (m.sla_percent || 0) >= 60 ? '#f59e0b' : '#ef4444',
                      }}
                    >
                      {(m.sla_percent || 0).toFixed(1)}%
                    </span>
                  </div>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Потеряно</span>
                    <span
                      style={{
                        ...styles.queueMetricValue,
                        color: (m.abandonment_rate || 0) <= 5 ? '#10b981' : (m.abandonment_rate || 0) <= 10 ? '#f59e0b' : '#ef4444',
                      }}
                    >
                      {(m.abandonment_rate || 0).toFixed(1)}%
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div style={styles.emptyChart}>Нет данных</div>
      )}

      <h2 style={styles.sectionTitle}>Детали по очередям (за 10 минут)</h2>
      {queues10m.length > 0 ? (
        <div style={styles.queueGrid}>
          {queues10m.map((queue) => {
            const m = windowData10m[queue];
            if (!m) return null;
            return (
              <div key={queue} style={styles.queueCard}>
                <h3 style={styles.queueTitle}>{queueNames[queue] || queue}</h3>
                <div style={styles.queueMetrics}>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Звонков</span>
                    <span style={styles.queueMetricValue}>{Math.round(m.calls_count || 0)}</span>
                  </div>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Ср. ожидание</span>
                    <span style={styles.queueMetricValue}>{Math.round(m.avg_wait_seconds || 0)} сек</span>
                  </div>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Уровень сервиса</span>
                    <span
                      style={{
                        ...styles.queueMetricValue,
                        color: (m.sla_percent || 0) >= 80 ? '#10b981' : (m.sla_percent || 0) >= 60 ? '#f59e0b' : '#ef4444',
                      }}
                    >
                      {(m.sla_percent || 0).toFixed(1)}%
                    </span>
                  </div>
                  <div style={styles.queueMetric}>
                    <span style={styles.queueMetricLabel}>Потеряно</span>
                    <span
                      style={{
                        ...styles.queueMetricValue,
                        color: (m.abandonment_rate || 0) <= 5 ? '#10b981' : (m.abandonment_rate || 0) <= 10 ? '#f59e0b' : '#ef4444',
                      }}
                    >
                      {(m.abandonment_rate || 0).toFixed(1)}%
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div style={styles.emptyChart}>Нет данных</div>
      )}
    </div>
  );
};

const AgentsTab = ({ agents }: { agents: Agent[] }) => {
  const queueNames: Record<string, string> = {
    support: 'Поддержка',
    sales: 'Продажи',
    billing: 'Биллинг',
    'tech-support': 'Тех. поддержка',
    vip: 'VIP',
  };

  if (agents.length === 0) {
    return (
      <div style={styles.emptyState}>
        <p>Нет данных об операторах</p>
      </div>
    );
  }

  return (
    <div>
      <h2 style={styles.sectionTitle}>Список операторов</h2>
      <p style={styles.hint}>Справочник операторов контакт-центра</p>
      <div style={styles.tableContainer}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>ID</th>
              <th style={styles.th}>Имя</th>
              <th style={styles.th}>Email</th>
              <th style={styles.th}>Основная очередь</th>
              <th style={styles.th}>Навыки</th>
              <th style={styles.th}>Дата найма</th>
            </tr>
          </thead>
          <tbody>
            {agents.map((agent) => (
              <tr key={agent.agent_id}>
                <td style={styles.td}>{agent.agent_id}</td>
                <td style={styles.td}>{agent.full_name}</td>
                <td style={styles.td}>{agent.email}</td>
                <td style={styles.td}>
                  <span style={styles.badge}>{queueNames[agent.primary_queue] || agent.primary_queue}</span>
                </td>
                <td style={styles.td}>
                  <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                    {agent.skills.map((skill) => (
                      <span key={skill} style={styles.skillBadge}>
                        {skill}
                      </span>
                    ))}
                  </div>
                </td>
                <td style={styles.td}>{agent.hire_date}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const MetricCard = ({
  label,
  value,
  suffix = '',
  color,
  icon,
  decimals = 0,
}: {
  label: string;
  value: number;
  suffix?: string;
  color: string;
  icon: string;
  decimals?: number;
}) => (
  <div style={styles.card}>
    <div style={styles.cardIcon}>{icon}</div>
    <div style={styles.cardContent}>
      <div style={styles.cardLabel}>{label}</div>
      <div style={{ ...styles.cardValue, color }}>
        {decimals > 0 ? value.toFixed(decimals) : value}
        {suffix}
      </div>
    </div>
  </div>
);

const formatValue = (key: string, value: number): string => {
  if (key.includes('percent') || key.includes('rate')) {
    return value.toFixed(1) + '%';
  }
  if (key === 'avg_sentiment') {
    return value.toFixed(2);
  }
  if (key === 'avg_customer_rating') {
    return value.toFixed(1) + ' из 5';
  }
  if (key.includes('seconds') || key.includes('time')) {
    return Math.round(value) + ' сек';
  }
  return Math.round(value).toString();
};

const styles: Record<string, React.CSSProperties> = {
  container: {
    minHeight: '100vh',
    backgroundColor: '#f8fafc',
    fontFamily: 'system-ui, -apple-system, sans-serif',
  },
  header: {
    backgroundColor: '#1e293b',
    color: '#fff',
    padding: '16px 24px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
  },
  headerContent: {
    maxWidth: 1400,
    margin: '0 auto',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  title: {
    margin: 0,
    fontSize: 24,
    fontWeight: 600,
  },
  headerRight: {
    display: 'flex',
    alignItems: 'center',
    gap: 24,
  },
  lastUpdate: {
    fontSize: 14,
    color: '#94a3b8',
  },
  nav: {
    backgroundColor: '#fff',
    borderBottom: '1px solid #e2e8f0',
    padding: '0 24px',
    display: 'flex',
    gap: 8,
    maxWidth: 1400,
    margin: '0 auto',
  },
  tabButton: {
    padding: '12px 20px',
    border: 'none',
    borderRadius: '8px 8px 0 0',
    cursor: 'pointer',
    fontSize: 14,
    fontWeight: 500,
    transition: 'all 0.2s',
  },
  main: {
    maxWidth: 1400,
    margin: '0 auto',
    padding: 24,
  },
  sectionTitle: {
    fontSize: 18,
    fontWeight: 600,
    color: '#1e293b',
    marginBottom: 16,
    marginTop: 32,
  },
  hint: {
    fontSize: 14,
    color: '#64748b',
    marginBottom: 16,
    marginTop: -8,
  },
  cardGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
    gap: 16,
  },
  card: {
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 20,
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    display: 'flex',
    alignItems: 'center',
    gap: 16,
  },
  cardIcon: {
    fontSize: 28,
  },
  cardContent: {
    flex: 1,
  },
  cardLabel: {
    fontSize: 13,
    fontWeight: 500,
    color: '#64748b',
    marginBottom: 4,
  },
  cardValue: {
    fontSize: 28,
    fontWeight: 700,
  },
  chartContainer: {
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 20,
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
  },
  emptyChart: {
    height: 200,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    color: '#94a3b8',
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 20,
    textAlign: 'center',
  },
  tableContainer: {
    backgroundColor: '#fff',
    borderRadius: 12,
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    overflow: 'hidden',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  th: {
    textAlign: 'left',
    padding: '12px 16px',
    backgroundColor: '#f8fafc',
    borderBottom: '1px solid #e2e8f0',
    fontSize: 13,
    fontWeight: 600,
    color: '#475569',
  },
  td: {
    padding: '10px 16px',
    borderBottom: '1px solid #f1f5f9',
    fontSize: 13,
    color: '#334155',
  },
  tdValue: {
    padding: '10px 16px',
    borderBottom: '1px solid #f1f5f9',
    fontSize: 13,
    color: '#1e293b',
    fontWeight: 500,
    textAlign: 'right',
  },
  footnote: {
    padding: '12px 16px',
    fontSize: 12,
    color: '#94a3b8',
    textAlign: 'right',
    borderTop: '1px solid #f1f5f9',
  },
  queueGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
    gap: 16,
  },
  queueCard: {
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 20,
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
  },
  queueTitle: {
    margin: '0 0 16px 0',
    fontSize: 16,
    fontWeight: 600,
    color: '#1e293b',
  },
  queueMetrics: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
    gap: 12,
  },
  queueMetric: {
    display: 'flex',
    flexDirection: 'column',
    gap: 4,
  },
  queueMetricLabel: {
    fontSize: 12,
    color: '#64748b',
  },
  queueMetricValue: {
    fontSize: 18,
    fontWeight: 600,
    color: '#1e293b',
  },
  badge: {
    display: 'inline-block',
    padding: '4px 10px',
    backgroundColor: '#e0f2fe',
    color: '#0369a1',
    borderRadius: 6,
    fontSize: 12,
    fontWeight: 500,
  },
  skillBadge: {
    display: 'inline-block',
    padding: '2px 8px',
    backgroundColor: '#f1f5f9',
    color: '#475569',
    borderRadius: 4,
    fontSize: 11,
  },
  emptyState: {
    textAlign: 'center',
    padding: 48,
    color: '#64748b',
  },
};

const styleSheet = document.createElement('style');
styleSheet.textContent = `
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
  }
  * { box-sizing: border-box; }
  body { margin: 0; }
`;
document.head.appendChild(styleSheet);
