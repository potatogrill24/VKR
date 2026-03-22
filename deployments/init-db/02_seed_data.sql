-- 02_seed_data.sql
-- Начальные данные для справочников

-- Очереди контакт-центра
INSERT INTO queues (queue_id, queue_name, description, sla_threshold_seconds, priority) VALUES
    ('support', 'Техническая поддержка', 'Общие вопросы по продуктам и услугам', 20, 2),
    ('sales', 'Продажи', 'Консультации по продуктам, оформление заказов', 30, 1),
    ('billing', 'Биллинг', 'Вопросы по оплате, счетам, возвратам', 20, 2),
    ('tech-support', 'Технический отдел', 'Сложные технические вопросы, эскалации', 45, 3),
    ('vip', 'VIP-клиенты', 'Приоритетное обслуживание VIP-клиентов', 10, 1)
ON CONFLICT (queue_id) DO NOTHING;

-- Операторы контакт-центра (из agents.csv)
INSERT INTO agents (agent_id, full_name, email, primary_queue, skills, hire_date, is_active) VALUES
    ('agent-1', 'Иван Петров', 'ivan.petrov@example.com', 'support', ARRAY['support', 'billing', 'english'], '2024-01-15', true),
    ('agent-2', 'Мария Иванова', 'maria.ivanova@example.com', 'sales', ARRAY['sales', 'english', 'vip'], '2024-02-01', true),
    ('agent-3', 'Алексей Сидоров', 'alexey.sidorov@example.com', 'billing', ARRAY['billing', 'support'], '2024-02-15', true),
    ('agent-4', 'Елена Козлова', 'elena.kozlova@example.com', 'sales', ARRAY['sales', 'russian'], '2024-03-01', true),
    ('agent-5', 'Дмитрий Смирнов', 'dmitry.smirnov@example.com', 'tech-support', ARRAY['tech-support', 'english', 'chinese'], '2024-03-10', true),
    ('agent-6', 'Анна Попова', 'anna.popova@example.com', 'support', ARRAY['support', 'russian'], '2024-03-15', true),
    ('agent-7', 'Сергей Васильев', 'sergey.vasiliev@example.com', 'billing', ARRAY['billing', 'english'], '2024-04-01', true),
    ('agent-8', 'Ольга Морозова', 'olga.morozova@example.com', 'sales', ARRAY['sales', 'vip', 'english'], '2024-04-15', true),
    ('agent-9', 'Павел Новиков', 'pavel.novikov@example.com', 'tech-support', ARRAY['tech-support', 'russian'], '2024-05-01', true),
    ('agent-10', 'Наталья Волкова', 'natalia.volkova@example.com', 'support', ARRAY['support', 'billing'], '2024-05-15', true),
    ('agent-11', 'Михаил Федоров', 'mikhail.fedorov@example.com', 'billing', ARRAY['billing', 'english'], '2024-06-01', true),
    ('agent-12', 'Татьяна Соколова', 'tatiana.sokolova@example.com', 'sales', ARRAY['sales', 'english', 'french'], '2024-06-15', true),
    ('agent-13', 'Андрей Михайлов', 'andrey.mikhailov@example.com', 'tech-support', ARRAY['tech-support', 'english'], '2024-07-01', true),
    ('agent-14', 'Юлия Егорова', 'ulia.egorova@example.com', 'support', ARRAY['support', 'russian'], '2024-07-15', true),
    ('agent-15', 'Виктор Павлов', 'viktor.pavlov@example.com', 'billing', ARRAY['billing', 'english'], '2024-08-01', true),
    ('agent-16', 'Ксения Степанова', 'kseniya.stepanova@example.com', 'sales', ARRAY['sales', 'vip'], '2024-08-15', true),
    ('agent-17', 'Григорий Николаев', 'grigoriy.nikolaev@example.com', 'tech-support', ARRAY['tech-support', 'russian'], '2024-09-01', true),
    ('agent-18', 'Вероника Ковалева', 'veronika.kovaleva@example.com', 'support', ARRAY['support', 'english'], '2024-09-15', true),
    ('agent-19', 'Илья Захаров', 'ilya.zakharov@example.com', 'billing', ARRAY['billing', 'russian'], '2024-10-01', true),
    ('agent-20', 'София Медведева', 'sofiya.medvedeva@example.com', 'sales', ARRAY['sales', 'english', 'spanish'], '2024-10-15', true)
ON CONFLICT (agent_id) DO UPDATE SET
    full_name = EXCLUDED.full_name,
    email = EXCLUDED.email,
    primary_queue = EXCLUDED.primary_queue,
    skills = EXCLUDED.skills,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();
