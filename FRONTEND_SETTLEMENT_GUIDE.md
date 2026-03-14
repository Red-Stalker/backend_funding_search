# Settlement Markers — Frontend Guide

## Изменения в API

### `/api/funding/history` — поле `settlements`

Ответ включает `settlements` — объект с массивами timestamp (ms)
реальных выплат фандинга для каждой биржи:

```json
{
  "series": { ... },
  "settlements": {
    "binance": [1741737600000, 1741766400000, 1741795200000],
    "hyperliquid": [1741737600000, 1741741200000, 1741744800000]
  }
}
```

### Бакеты

- ≤7d — 1h бакеты
- 14d/30d — 4h бакеты

Для 8h бирж между settlement-ами идёт forward-fill (горизонтальная линия до 9h gap),
что корректно — ставка постоянна между исполнениями.

Рекомендация: использовать `type="stepAfter"` для `Line` в Recharts.

## Как отобразить на графике (Recharts)

### Вариант 1: ReferenceDot на каждый settlement

```jsx
import { ReferenceDot } from 'recharts';

{Object.entries(settlements).map(([exchange, timestamps]) =>
  timestamps
    .filter(ts => ts >= startMs && ts <= endMs)
    .map(ts => (
      <ReferenceDot
        key={`${exchange}-${ts}`}
        x={ts}
        y={/* найти значение rate в series[exchange] для этого ts */}
        r={4}
        fill={EXCHANGE_COLORS[exchange]}
        stroke="#fff"
        strokeWidth={1}
      />
    ))
)}
```

### Вариант 2: ReferenceLine вертикальная на каждый settlement

```jsx
{settlements.binance?.map(ts => (
  <ReferenceLine
    key={ts}
    x={ts}
    stroke="rgba(255,255,255,0.15)"
    strokeDasharray="3 3"
  />
))}
```

### Вариант 3: Комбинация — линии + точки

Вертикальные пунктирные линии показывают КОГДА произошёл settlement,
а точки на линиях графика показывают КАКАЯ ставка была исполнена.

## Архитектура данных

### Два job-а собирают данные:

- **Snapshots** (каждые 60 мин): batch endpoints → `current_rates`. Для live dashboard. Только биржи с batch endpoint.
- **Settlements** (каждые 60 мин, +30 мин offset): per-symbol `fetch_funding_history()` → `funding_rates` + `current_rates`. Реальные settlement timestamps от бирж.

### Таблицы:

- **`current_rates`** — обновляется обоими job-ами. Содержит predicted/текущие ставки для live-дашборда.
- **`funding_rates`** — содержит ТОЛЬКО реальные settlement rates с настоящими timestamp от бирж. Используется для history/scan/APR.

### Interval map в API:

History endpoint передаёт **storage interval** (8 для нормализованных, native для drift).
Это корректно для формул расчёта, т.к. данные в БД хранятся в 8h BPS.
