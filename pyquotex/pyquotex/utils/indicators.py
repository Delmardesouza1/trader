import statistics


class TechnicalIndicators:
    @staticmethod
    def calculate_sma(prices: list[float], period: int) -> list[float]:
        """Calcula la Media Móvil Simple (SMA)"""
        if len(prices) < period:
            return []

        sma_values = []
        for i in range(len(prices) - period + 1):
            sma = sum(prices[i: (i + period)]) / period
            sma_values.append(sma)
        return sma_values

    @staticmethod
    def calculate_ema(prices: list[float], period: int) -> list[float]:
        """Calcula la Media Móvil Exponencial (EMA)"""
        if len(prices) < period:
            return []

        multiplier = 2 / (period + 1)
        ema_values = [sum(prices[:period]) / period]

        for price in prices[period:]:
            ema = (price * multiplier) + (ema_values[-1] * (1 - multiplier))
            ema_values.append(ema)
        return ema_values

    @staticmethod
    def calculate_rsi(prices: list[float], period: int = 14) -> list[float]:
        """Calcula el Índice de Fuerza Relativa (RSI)"""
        if len(prices) < period + 1:
            return []

        # Pure-Python replacement for numpy: no external dependency needed.
        deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        gain = [d if d > 0 else 0.0 for d in deltas]
        loss = [-d if d < 0 else 0.0 for d in deltas]

        avg_gain = sum(gain[:period]) / period
        avg_loss = sum(loss[:period]) / period

        rsi_values: list[float] = []

        # Calculate initial RSI for the first period
        rs = avg_gain / (avg_loss if avg_loss != 0 else 0.00001)
        rsi_values.append(100 - (100 / (1 + rs)))

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gain[i]) / period
            avg_loss = (avg_loss * (period - 1) + loss[i]) / period
            rs = avg_gain / (avg_loss if avg_loss != 0 else 0.00001)
            rsi_values.append(100 - (100 / (1 + rs)))

        return rsi_values

    @staticmethod
    def calculate_macd(
            prices: list[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9
    ) -> dict[str, list[float] | dict[str, float | None]]:
        """Calcula el MACD (Moving Average Convergence Divergence)"""
        if len(prices) < slow_period:
            return {"macd": [], "signal": [], "histogram": []}

        fast_ema = TechnicalIndicators.calculate_ema(prices, fast_period)
        slow_ema = TechnicalIndicators.calculate_ema(prices, slow_period)

        macd_line = []
        for i in range(len(slow_ema)):
            macd = fast_ema[i + (len(fast_ema) - len(slow_ema))] - slow_ema[i]
            macd_line.append(macd)

        signal_line = TechnicalIndicators.calculate_ema(macd_line, signal_period)

        histogram = []
        for i in range(len(signal_line)):
            hist = macd_line[i + (len(macd_line) - len(signal_line))] - signal_line[i]
            histogram.append(hist)

        return {
            "macd": macd_line,
            "signal": signal_line,
            "histogram": histogram,
            "current": {
                "macd": macd_line[-1] if macd_line else None,
                "signal": signal_line[-1] if signal_line else None,
                "histogram": histogram[-1] if histogram else None,
            },
        }

    @staticmethod
    def calculate_bollinger_bands(
            prices: list[float], period: int = 20, num_std: float = 2
    ) -> dict[str, list[float] | dict[str, float | None]]:
        """Calcula las Bandas de Bollinger"""
        if len(prices) < period:
            return {"upper": [], "middle": [], "lower": []}

        sma = TechnicalIndicators.calculate_sma(prices, period)
        std = []

        for i in range(len(prices) - period + 1):
            window = prices[i: (i + period)]
            # statistics.pstdev is population std dev — same as numpy.std default
            std.append(statistics.pstdev(window))

        upper_band = [sma[i] + (std[i] * num_std) for i in range(len(sma))]
        lower_band = [sma[i] - (std[i] * num_std) for i in range(len(sma))]

        return {
            "upper": [x for x in upper_band],
            "middle": [x for x in sma],
            "lower": [x for x in lower_band],
            "current": {
                "upper": upper_band[-1] if upper_band else None,
                "middle": sma[-1] if sma else None,
                "lower": lower_band[-1] if lower_band else None,
            },
        }

    @staticmethod
    def calculate_stochastic(
            prices: list[float],
            highs: list[float],
            lows: list[float],
            k_period: int = 14,
            d_period: int = 3,
    ) -> dict[str, list[float] | dict[str, float | None]]:
        """Calcula el Oscilador Estocástico"""
        if len(prices) < k_period:
            return {"k": [], "d": []}

        k_values = []

        for i in range(len(prices) - k_period + 1):
            window_high = max(highs[i: i + k_period])
            window_low = min(lows[i: i + k_period])

            if window_high == window_low:
                k = 100
            else:
                k = ((prices[i + k_period - 1] - window_low) / (window_high - window_low)) * 100
            k_values.append(k)

        d_values = TechnicalIndicators.calculate_sma(k_values, d_period)

        return {
            "k": k_values,
            "d": d_values,
            "current": {
                "k": k_values[-1] if k_values else None,
                "d": d_values[-1] if d_values else None,
            },
        }

    @staticmethod
    def calculate_atr(
            highs: list[float], lows: list[float], closes: list[float], period: int = 14
    ) -> list[float]:
        """Calcula el Average True Range (ATR)"""
        if len(highs) < period:
            return []

        true_ranges = []
        for i in range(1, len(highs)):
            high = highs[i]
            low = lows[i]
            prev_close = closes[i - 1]

            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)

            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)

        atr_values = [sum(true_ranges[:period]) / period]

        for i in range(period, len(true_ranges)):
            atr = (atr_values[-1] * (period - 1) + true_ranges[i]) / period
            atr_values.append(atr)

        return atr_values

    @staticmethod
    def calculate_adx(
            highs: list[float], lows: list[float], closes: list[float], period: int = 14
    ) -> dict[str, list[float] | dict[str, float | None]]:
        """Calcula el Average Directional Index (ADX)"""
        if len(highs) < period + 1:
            return {"adx": [], "plus_di": [], "minus_di": []}

        # Calcular True Range
        tr = []
        plus_dm = []
        minus_dm = []

        for i in range(1, len(highs)):
            high = highs[i]
            low = lows[i]
            prev_high = highs[i - 1]
            prev_low = lows[i - 1]
            prev_close = closes[i - 1]

            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            tr.append(max(tr1, tr2, tr3))

            plus_dm1 = high - prev_high
            minus_dm1 = prev_low - low

            if plus_dm1 > minus_dm1 and plus_dm1 > 0:
                plus_dm.append(plus_dm1)
            else:
                plus_dm.append(0)

            if minus_dm1 > plus_dm1 and minus_dm1 > 0:
                minus_dm.append(minus_dm1)
            else:
                minus_dm.append(0)

        # Calcular los promedios
        def _safe_div(a: float, b: float) -> float:
            return a / b if b else 0.0

        # Wilder: suaviza TR, +DM e -DM separadamente e só então calcula os DIs.
        tr_avg = [sum(tr[:period]) / period]
        plus_dm_avg = [sum(plus_dm[:period]) / period]
        minus_dm_avg = [sum(minus_dm[:period]) / period]

        for i in range(period, len(tr)):
            tr_avg.append((tr_avg[-1] * (period - 1) + tr[i]) / period)
            plus_dm_avg.append((plus_dm_avg[-1] * (period - 1) + plus_dm[i]) / period)
            minus_dm_avg.append((minus_dm_avg[-1] * (period - 1) + minus_dm[i]) / period)

        plus_di_avg = [_safe_div(p * 100, t) for p, t in zip(plus_dm_avg, tr_avg)]
        minus_di_avg = [_safe_div(m * 100, t) for m, t in zip(minus_dm_avg, tr_avg)]

        # Calcular ADX
        dx_values = []
        for i in range(len(plus_di_avg)):
            dx = _safe_div(abs(plus_di_avg[i] - minus_di_avg[i]), plus_di_avg[i] + minus_di_avg[i]) * 100
            dx_values.append(dx)
        if len(dx_values) < period:
            return {"adx": [], "plus_di": plus_di_avg, "minus_di": minus_di_avg,
                    "current": {"adx": None, "plus_di": None, "minus_di": None}}

        adx_values = [sum(dx_values[:period]) / period]
        for i in range(period, len(dx_values)):
            adx = (adx_values[-1] * (period - 1) + dx_values[i]) / period
            adx_values.append(adx)

        return {
            "adx": adx_values,
            "plus_di": [x for x in plus_di_avg],
            "minus_di": [x for x in minus_di_avg],
            "current": {
                "adx": adx_values[-1] if adx_values else None,
                "plus_di": plus_di_avg[-1] if plus_di_avg else None,
                "minus_di": minus_di_avg[-1] if minus_di_avg else None,
            },
        }

    @staticmethod
    def calculate_ichimoku(
            highs: list[float],
            lows: list[float],
            tenkan_period: int = 9,
            kijun_period: int = 26,
            senkou_b_period: int = 52,
            closes: list[float] | None = None,
    ) -> dict[str, list[float] | dict[str, float | None]]:
        """Calcula el Ichimoku Cloud"""
        if len(highs) < senkou_b_period:
            return {"tenkan": [], "kijun": [], "senkou_a": [], "senkou_b": [], "chikou": []}

        def donchian(high_prices: list[float], low_prices: list[float], period: int) -> list[float]:
            result = []
            for i in range(len(high_prices) - period + 1):
                highest = max(high_prices[i: i + period])
                lowest = min(low_prices[i: i + period])
                result.append((highest + lowest) / 2)
            return result

        # Cálculo de las líneas
        tenkan = donchian(highs, lows, tenkan_period)
        kijun = donchian(highs, lows, kijun_period)
        senkou_b = donchian(highs, lows, senkou_b_period)

        # Senkou Span A (Promedio de Tenkan y Kijun)
        senkou_a = []
        for i in range(min(len(tenkan), len(kijun))):
            senkou_a.append((tenkan[i] + kijun[i]) / 2)

        # Chikou Span (Precio de cierre desplazado 26 períodos hacia atrás)
        # Chikou é o FECHAMENTO deslocado; sem closes, usa o ponto médio da vela.
        if closes is None:
            closes = [(h + lo) / 2 for h, lo in zip(highs, lows)]
        chikou = closes[kijun_period:]

        return {
            "tenkan": [x for x in tenkan],
            "kijun": [x for x in kijun],
            "senkou_a": [x for x in senkou_a],
            "senkou_b": [x for x in senkou_b],
            "chikou": [x for x in chikou],
            "current": {
                "tenkan": tenkan[-1] if tenkan else None,
                "kijun": kijun[-1] if kijun else None,
                "senkou_a": senkou_a[-1] if senkou_a else None,
                "senkou_b": senkou_b[-1] if senkou_b else None,
                "chikou": chikou[-1] if chikou else None,
            },
        }
