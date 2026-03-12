# Settlement Markers — Frontend Guide

## Изменения в API

### `/api/funding/history` — новое поле `settlements`

Ответ теперь включает `settlements` — объект с массивами timestamp (ms)
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

Все таймфреймы (1d, 7d, 14d, 30d) теперь используют часовые бакеты.
Для 8h бирж между settlement-ами идёт forward-fill (горизонтальная линия),
что корректно — ставка постоянна между исполнениями.

Рекомендация: использовать `type="stepAfter"` для `Line` в Recharts,
чтобы визуально показать что ставка держится до следующего settlement.

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

## Снепшоты vs Settlements

- **`current_rates`** — обновляется каждый час snapshot-ом. Содержит predicted/текущие ставки для live-дашборда.
- **`funding_rates`** — содержит ТОЛЬКО реальные settlement rates с настоящими timestamp от бирж. Используется для history/scan/APR.
