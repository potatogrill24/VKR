import { useEffect, useState, useCallback } from 'react';

const API_BASE = 'http://localhost:8081';
const WS_URL = 'ws://localhost:8080/ws/realtime';

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

export const App = () => {
  const [realtime, setRealtime] = useState<RealtimeMetrics | null>(null);
  const [latestMetrics, setLatestMetrics] = useState<LatestMetrics>({});
  const [queueMetrics, setQueueMetrics] = useState<QueueMetricsByWindow>({});
  const [agents, setAgents] = useState<Agent[]>([]);
  const [wsStatus, setWsStatus] = useState<'connecting' | 'open' | 'closed' | 'error'>('connecting');
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [activeTab, setActiveTab] = useState<'realtime' | 'global' | 'queues' | 'agents'>('realtime');

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

  useEffect(() => {
    fetchLatestMetrics();
    fetchQueueMetrics();
    fetchAgents();

    const interval = setInterval(() => {
      fetchLatestMetrics();
      fetchQueueMetrics();
    }, 30000);

    return () => clearInterval(interval);
  }, [fetchLatestMetrics, fetchQueueMetrics, fetchAgents]);

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
        <TabButton active={activeTab === 'global'} onClick={() => setActiveTab('global')}>
          Глобальные метрики
        </TabButton>
        <TabButton active={activeTab === 'queues'} onClick={() => setActiveTab('queues')}>
          По очередям
        </TabButton>
        <TabButton active={activeTab === 'agents'} onClick={() => setActiveTab('agents')}>
          Операторы
        </TabButton>
      </nav>

      <main style={styles.main}>
        {activeTab === 'realtime' && <RealtimeTab realtime={realtime} />}
        {activeTab === 'global' && <GlobalTab metrics={latestMetrics} />}
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

const RealtimeTab = ({ realtime }: { realtime: RealtimeMetrics | null }) => {
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

  return (
    <div>
      <h2 style={styles.sectionTitle}>Состояние операторов</h2>
      <p style={styles.hint}>
        Показывает текущее состояние операторов на основе последних событий
      </p>
      <div style={styles.cardGrid}>
        <MetricCard
          label="Свободны"
          hint="Операторы, готовые принять звонок"
          value={realtime.agents_available}
          color="#10b981"
          icon="👤"
        />
        <MetricCard
          label="На линии"
          hint="Операторы, разговаривающие с клиентом"
          value={realtime.agents_in_call}
          color="#3b82f6"
          icon="📞"
        />
        <MetricCard
          label="Обработка"
          hint="Послеразговорная обработка (wrap-up)"
          value={realtime.agents_wrap_up}
          color="#f59e0b"
          icon="📝"
        />
      </div>

      <h2 style={styles.sectionTitle}>Состояние очереди</h2>
      <p style={styles.hint}>
        Метрики ожидания клиентов за последние 5 минут
      </p>
      <div style={styles.cardGrid}>
        <MetricCard
          label="В очереди"
          hint="Клиенты, ожидающие ответа"
          value={realtime.calls_in_queue}
          color="#8b5cf6"
          icon="📋"
        />
        <MetricCard
          label="Макс. ожидание"
          hint="Самое долгое ожидание за 5 мин"
          value={realtime.longest_wait_sec}
          suffix=" сек"
          color={realtime.longest_wait_sec > 30 ? '#ef4444' : '#10b981'}
          icon="⏱️"
        />
        <MetricCard
          label="Среднее ожидание"
          hint="Среднее время в очереди за 5 мин"
          value={realtime.avg_wait_sec}
          suffix=" сек"
          color={realtime.avg_wait_sec > 20 ? '#f59e0b' : '#10b981'}
          icon="⏳"
        />
        <MetricCard
          label="Звонков/мин"
          hint="Интенсивность входящих звонков"
          value={realtime.calls_per_minute}
          color="#06b6d4"
          icon="📈"
        />
      </div>

      <h2 style={styles.sectionTitle}>Качество обслуживания (за 5 минут)</h2>
      <p style={styles.hint}>
        Ключевые показатели эффективности контакт-центра
      </p>
      <div style={styles.cardGrid}>
        <MetricCard
          label="Уровень сервиса (SL)"
          hint="% звонков, отвеченных за 20 сек (SLA)"
          value={realtime.service_level}
          suffix="%"
          color={realtime.service_level >= 80 ? '#10b981' : realtime.service_level >= 60 ? '#f59e0b' : '#ef4444'}
          icon="🎯"
          decimals={1}
        />
        <MetricCard
          label="Потерянные звонки"
          hint="% клиентов, не дождавшихся ответа"
          value={realtime.abandonment_rate}
          suffix="%"
          color={realtime.abandonment_rate <= 5 ? '#10b981' : realtime.abandonment_rate <= 10 ? '#f59e0b' : '#ef4444'}
          icon="📉"
          decimals={1}
        />
      </div>
    </div>
  );
};

const GlobalTab = ({ metrics }: { metrics: LatestMetrics }) => {
  const windows = ['2m', '10m'];
  const windowLabels: Record<string, string> = {
    '2m': 'За 2 минуты',
    '10m': 'За 10 минут',
  };
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

  const metricHints: Record<string, string> = {
    calls_count: 'Общее число звонков за период',
    avg_wait_seconds: 'Сколько в среднем клиент ждёт в очереди',
    avg_talk_seconds: 'Средняя продолжительность разговора',
    avg_handle_time: 'Ожидание + разговор + удержание + обработка',
    sla_percent: '% звонков, отвеченных в пределах SLA (20 сек)',
    abandonment_rate: '% клиентов, бросивших трубку',
    transfer_rate: '% звонков, переведённых на другого оператора',
    fcr_rate: '% вопросов, решённых с первого обращения',
    avg_customer_rating: 'Средняя оценка от 1 до 5',
    avg_sentiment: 'Анализ тональности: от -1 (негатив) до +1 (позитив)',
  };

  const hasData = Object.keys(metrics).length > 0;

  if (!hasData) {
    return (
      <div style={styles.emptyState}>
        <p>Нет агрегированных данных</p>
        <p style={{ fontSize: 14, color: '#94a3b8' }}>
          Данные появятся после накопления истории звонков (обновление каждые 2 минуты)
        </p>
      </div>
    );
  }

  return (
    <div>
      <p style={styles.hint}>
        Агрегированные метрики за последние 2 и 10 минут. Обновляются каждые 2 минуты.
      </p>
      <div style={styles.tableContainer}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>Метрика</th>
              {windows.map((w) => (
                <th key={w} style={styles.th}>
                  {windowLabels[w]}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Object.entries(metricNames).map(([key, label]) => (
              <tr key={key}>
                <td style={styles.td}>
                  <div>{label}</div>
                  <div style={{ fontSize: 12, color: '#94a3b8' }}>{metricHints[key]}</div>
                </td>
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
      </div>

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

  const windows = ['2m', '10m'];
  const windowLabels: Record<string, string> = {
    '2m': 'За последние 2 минуты',
    '10m': 'За последние 10 минут',
  };

  const hasData = Object.keys(metrics).length > 0;

  if (!hasData) {
    return (
      <div style={styles.emptyState}>
        <p>Нет данных по очередям</p>
        <p style={{ fontSize: 14, color: '#94a3b8' }}>
          Данные появятся после накопления истории звонков
        </p>
      </div>
    );
  }

  return (
    <div>
      <p style={styles.hint}>
        Метрики по каждой очереди. Обновляются каждые 2 минуты.
      </p>

      {windows.map((window) => {
        const windowData = metrics[window];
        if (!windowData) return null;

        const queues = Object.keys(windowData);
        if (queues.length === 0) return null;

        return (
          <div key={window}>
            <h2 style={styles.sectionTitle}>{windowLabels[window]}</h2>
            <div style={styles.queueGrid}>
              {queues.map((queue) => {
                const m = windowData[queue];
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
          </div>
        );
      })}
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
      <h2 style={styles.sectionTitle}>Операторы ({agents.length})</h2>
      <p style={styles.hint}>
        Справочник операторов контакт-центра
      </p>
      <div style={styles.tableContainer}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>ID</th>
              <th style={styles.th}>Имя</th>
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
  hint,
  value,
  suffix = '',
  color,
  icon,
  decimals = 0,
}: {
  label: string;
  hint?: string;
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
      {hint && <div style={styles.cardHint}>{hint}</div>}
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
    marginBottom: 8,
    marginTop: 32,
  },
  hint: {
    fontSize: 14,
    color: '#64748b',
    marginBottom: 16,
    marginTop: 0,
  },
  cardGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
    gap: 16,
  },
  card: {
    backgroundColor: '#fff',
    borderRadius: 12,
    padding: 20,
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    display: 'flex',
    alignItems: 'flex-start',
    gap: 16,
  },
  cardIcon: {
    fontSize: 32,
  },
  cardContent: {
    flex: 1,
  },
  cardLabel: {
    fontSize: 14,
    fontWeight: 500,
    color: '#334155',
    marginBottom: 2,
  },
  cardHint: {
    fontSize: 12,
    color: '#94a3b8',
    marginBottom: 8,
  },
  cardValue: {
    fontSize: 28,
    fontWeight: 700,
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
    padding: '14px 16px',
    backgroundColor: '#f8fafc',
    borderBottom: '1px solid #e2e8f0',
    fontSize: 14,
    fontWeight: 600,
    color: '#475569',
  },
  td: {
    padding: '12px 16px',
    borderBottom: '1px solid #f1f5f9',
    fontSize: 14,
    color: '#334155',
    verticalAlign: 'top',
  },
  tdValue: {
    padding: '12px 16px',
    borderBottom: '1px solid #f1f5f9',
    fontSize: 14,
    color: '#1e293b',
    fontWeight: 500,
    textAlign: 'right',
    verticalAlign: 'middle',
  },
  footnote: {
    marginTop: 12,
    fontSize: 13,
    color: '#94a3b8',
    textAlign: 'right',
  },
  queueGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
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
    fontSize: 13,
    fontWeight: 500,
  },
  skillBadge: {
    display: 'inline-block',
    padding: '2px 8px',
    backgroundColor: '#f1f5f9',
    color: '#475569',
    borderRadius: 4,
    fontSize: 12,
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
