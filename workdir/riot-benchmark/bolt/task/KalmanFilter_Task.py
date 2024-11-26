from properties import DEFAULT_KEY, kalmanFilter_usemsgField, kalmanFilter_processNoise, kalmanFilter_sensorNoise, kalmanFilter_estimatedError
def doTask(hash_map):
    m = hash_map[DEFAULT_KEY]
    z_measuredValue = 0.0
    q_processNoise = kalmanFilter_processNoise
    r_sensorNoise = kalmanFilter_sensorNoise
    p0_priorErrorCovariance = kalmanFilter_estimatedError
    x0_previousEstimation = 0.1
    if kalmanFilter_usemsgField > 0:
        z_measuredValue = m.split(',')[kalmanFilter_usemsgField - 1]
    else:
        z_measuredValue = float(m)

    p1_currentErrorCovariance = p0_priorErrorCovariance + q_processNoise
    k_kalmanGain = p1_currentErrorCovariance / (p1_currentErrorCovariance + r_sensorNoise)
    x1_currentEstimation = x0_previousEstimation + k_kalmanGain * (z_measuredValue - x0_previousEstimation)
    p1_currentErrorCovariance = (1 - k_kalmanGain) * p1_currentErrorCovariance

    currentEstimation = x1_currentEstimation
    return currentEstimation
    
