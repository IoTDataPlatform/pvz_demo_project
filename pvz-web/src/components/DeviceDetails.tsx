import type { DeviceState } from '../api/types';
import './DeviceDetails.css';

type Props = {
    device: DeviceState | null;
};

function formatTs(ts: number | null): string {
    if (!ts) return '-';
    const d = new Date(ts * 1000);
    return d.toLocaleString();
}

const DeviceDetails = ({ device }: Props) => {
    if (!device) {
        return (
            <div className="device-details">
                <h2>Информация по устройству</h2>
                <p>Выберите устройство на карте.</p>
            </div>
        );
    }

    return (
        <div className="device-details">
            <h2>Устройство {device.deviceId}</h2>
            <p className="device-env">
                {device.env} / {device.tenantId}
            </p>

            <p>
                <strong>Влажность:</strong>{' '}
                {device.h != null ? `${device.h.toFixed(1)} %` : '-'}
            </p>
            <p>
                <strong>Температура:</strong>{' '}
                {device.t != null ? `${device.t.toFixed(1)} °C` : '-'}
            </p>
            <p>
                <strong>Онлайн:</strong>{' '}
                {device.online === true
                    ? 'да'
                    : device.online === false
                        ? 'нет'
                        : '-'}
            </p>
            <p>
                <strong>Батарея:</strong>{' '}
                {device.bat != null ? `${device.bat.toFixed(2)} В` : '-'}
            </p>
            <p>
                <strong>RSSI:</strong>{' '}
                {device.rssi != null ? `${device.rssi} dBm` : '-'}
            </p>
            <p>
                <strong>SNR:</strong>{' '}
                {device.snr != null ? `${device.snr.toFixed(1)} dB` : '-'}
            </p>

            <p>
                <strong>Последние данные (h/t):</strong> {formatTs(device.tsHt! / 1000)}
            </p>
            <p>
                <strong>Последнее состояние:</strong> {formatTs(device.tsState! / 1000)}
            </p>
        </div>
    );
};

export default DeviceDetails;
