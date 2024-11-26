from properties_STAT import kalmanFilterUseMsgFields, kalmanFilterUseMsgField, kalmanFilterEstimatedError, kalmanFilterProcessNoise, kalmanFilterSensorNoise

class KalmanFilter:
    def __init__(self, val):
        self.x0_previousEstimation = val

    def filter(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        q_prosessNoise = kalmanFilterProcessNoise
        r_sensorNoise = kalmanFilterSensorNoise
        p0_priorErrorCovariance = kalmanFilterEstimatedError

        if obsType in kalmanFilterUseMsgFields:
            z_measuredValue = -1
            if kalmanFilterUseMsgField > 0:
                z_measuredValue = float(obsVal.split(',')[kalmanFilterUseMsgField - 1])
            elif kalmanFilterUseMsgField == 0:
                z_measuredValue = float(obsVal)

            p1_currentErrorCovariance = p0_priorErrorCovariance + q_prosessNoise
            k_kalmanGain = p1_currentErrorCovariance / (p1_currentErrorCovariance + r_sensorNoise)
            x1_currentEstimation = self.x0_previousEstimation + k_kalmanGain * (z_measuredValue - self.x0_previousEstimation)
            p1_currentErrorCovariance = (1 - k_kalmanGain) * p1_currentErrorCovariance

            currentEstimation = float(x1_currentEstimation)

            return (msgId, sensorId, meta, obsType, str(currentEstimation))
        