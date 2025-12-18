import { useEffect, useState } from 'react';
import {
    ResponsiveContainer,
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
} from 'recharts';
import { fetchDeviceMetrics } from '../api/client';
import type { DeviceMetricsResponse, MetricsBucket } from '../api/types';
import './DeviceCharts.css';

type Props = {
    envName: string;
    tenantId: string;
    deviceId: string | null;
};

function toLocalInputValue(date: Date): string {
    const pad = (n: number) => n.toString().padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
        date.getDate()
    )}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

const DeviceCharts = ({ envName, tenantId, deviceId }: Props) => {
    const [bucket, setBucket] = useState<MetricsBucket>('hour');
    const [metrics, setMetrics] = useState<DeviceMetricsResponse | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const [from, setFrom] = useState<string>(() => {
        const now = new Date();
        const fromDate = new Date(now.getTime() - 24 * 3600 * 1000);
        return toLocalInputValue(fromDate);
    });

    const [to, setTo] = useState<string>(() => {
        const now = new Date();
        return toLocalInputValue(now);
    });


    useEffect(() => {
        if (!deviceId || !from || !to) {
            return;
        }

        const fromTs = Math.floor(new Date(from).getTime() / 1000);
        const toTs = Math.floor(new Date(to).getTime() / 1000);

        if (!Number.isFinite(fromTs) || !Number.isFinite(toTs) || fromTs >= toTs) {
            return;
        }

        let cancelled = false;

        async function load() {
            try {
                setLoading(true);
                setError(null);
                const data = await fetchDeviceMetrics(
                    envName,
                    tenantId,
                    deviceId!,
                    bucket,
                    fromTs,
                    toTs
                );
                if (!cancelled) {
                    setMetrics(data);
                }
            } catch (e: any) {
                if (!cancelled) {
                    setError(e?.message ?? 'Ошибка загрузки метрик');
                }
            } finally {
                if (!cancelled) {
                    setLoading(false);
                }
            }
        }

        load();

        return () => {
            cancelled = true;
        };
    }, [envName, tenantId, deviceId, bucket, from, to]);

    if (!deviceId) {
        return (
            <div className="device-charts">
                <h2>Графики</h2>
                <p>Выберите устройство на карте.</p>
            </div>
        );
    }

    const data =
        metrics?.points.map((p) => ({
            ts: p.ts * 1000,
            label: new Date(p.ts * 1000).toLocaleString(),
            tAvg: p.tAvg,
            hAvg: p.hAvg,
        })) ?? [];

    return (
        <div className="device-charts">
            <div className="charts-header">
                <h2>Температура / Влажность</h2>

                <div className="charts-controls">
                    <div className="charts-control-group">
                        <label htmlFor="bucket-select">Агрегация</label>
                        <select
                            id="bucket-select"
                            value={bucket}
                            onChange={(e) =>
                                setBucket(e.target.value as MetricsBucket)
                            }
                        >
                            <option value="hour">Часы</option>
                            <option value="day">Дни</option>
                            <option value="week">Недели</option>
                        </select>
                    </div>

                    <div className="charts-control-group">
                        <label htmlFor="from-input">От</label>
                        <input
                            id="from-input"
                            type="datetime-local"
                            value={from}
                            onChange={(e) => setFrom(e.target.value)}
                        />
                    </div>

                    <div className="charts-control-group">
                        <label htmlFor="to-input">До</label>
                        <input
                            id="to-input"
                            type="datetime-local"
                            value={to}
                            onChange={(e) => setTo(e.target.value)}
                        />
                    </div>
                </div>
            </div>

            {loading && <p>Загрузка графика…</p>}
            {error && <p className="charts-error">Ошибка: {error}</p>}

            {!loading && !error && data.length === 0 && (
                <p>Нет данных для отображения.</p>
            )}

            {!loading && !error && data.length > 0 && (
                <div className="chart-container">
                    <ResponsiveContainer width="100%" height={340}>
                        <LineChart data={data}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis
                                dataKey="label"
                                tick={{ fontSize: 13 }}
                                minTickGap={20}
                            />
                            <YAxis
                                yAxisId="left"
                                tick={{ fontSize: 13 }}
                                label={{
                                    value: 'Температура °C',
                                    angle: -90,
                                    position: 'insideLeft',
                                    style: { fontSize: 14 },
                                }}
                            />
                            <YAxis
                                yAxisId="right"
                                orientation="right"
                                tick={{ fontSize: 13 }}
                                label={{
                                    value: 'Влажность %',
                                    angle: 90,
                                    position: 'insideRight',
                                    style: { fontSize: 14 },
                                }}
                            />
                            <Tooltip
                                contentStyle={{
                                    backgroundColor: '#020617',
                                    borderRadius: 12,
                                    border: '1px solid #4b5563',
                                    boxShadow: '0 12px 30px rgba(0, 0, 0, 0.8)',
                                }}
                                labelStyle={{
                                    color: '#e5e7eb',
                                    fontSize: 20,
                                }}
                                itemStyle={{
                                    color: '#e5e7eb',
                                    fontSize: 20,
                                }}
                            />
                            <Legend wrapperStyle={{ fontSize: 13 }} />
                            <Line
                                yAxisId="left"
                                type="monotone"
                                dataKey="tAvg"
                                name="Температура"
                                stroke="#60a5fa"
                                dot={false}
                                strokeWidth={3}
                            />
                            <Line
                                yAxisId="right"
                                type="monotone"
                                dataKey="hAvg"
                                name="Влажность"
                                stroke="#34d399"
                                dot={false}
                                strokeWidth={3}
                            />
                        </LineChart>
                    </ResponsiveContainer>
                </div>
            )}
        </div>
    );
};

export default DeviceCharts;
