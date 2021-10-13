import csv
import os
import logging

def SEIRS_Model(initial_state: dict, 
    timesteps: float,
    years: float,
    alpha: float,
    beta: float,
    inv_gamma: float,
    inv_omega: float,
    inv_mu: float,
    inv_sigma: float
):
    """
    Implementation of the SEIRS Model

    Args:
        |   initial_state: dict
        |       's': S Parameter (Suceptible)
        |       'e': E Parameter (Exposed)
        |       'i': I Parameter (Infectious)
        |       'r': R Parameter (Recovered)
        |   timesteps: timesteps for model
        |   years: model timeframe years
        |   alpha: alpha parameter for model
        |   beta: beta parameter for model
        |   inv_gamma: inverse gamma parameter for model
        |   inv_omega: inverse omega parameter for model
        |   inv_mu: inverse mu parameter for model
        |   inv_sigma: inverse sigma parameter for model

    Returns:
        |   results: dict
        |      '0': dict
        |      'time': current timepoint
        |      'S': S Parameter at timepoint
        |      'E': E Parameter at timepoint
        |      'I': I Parameter at timepoint
        |      'R': R Paramerer at timepoint
    """
    # Check all inital states are present
    disease_states = ['s', 'e', 'i', 'r']
    for state in disease_states:
        if not state in initial_state:
            raise ValueError("Please supply " + state + " disease state")

    # Initialise SEIR
    S = initial_state['s']
    E = initial_state['e']
    I = initial_state['i']
    R = initial_state['r']

    # Prepare Time Units
    time_unit_years = years / timesteps
    time_unit_days = time_unit_years * 365.25

    # Convert Parameters to days
    alpha = alpha * time_unit_days
    beta = beta * time_unit_days
    gamma = time_unit_days / inv_gamma
    omega = time_unit_days / (inv_omega * 365.25)
    mu = time_unit_days / (inv_mu *365.25)
    sigma = time_unit_days / inv_sigma

    results = {}
    results[0] = {'time': 0, "S": S, "E": E, "I": I, "R": R}

    for i in range(timesteps):
        N = results[i]['S'] + results[i]['E'] + results[i]['I'] + results[i]['R']
        birth = mu * N
        infection = (beta * results[i]['I'] * results[i]['S']) / N
        lost_immunity = omega * results[i]['R']
        death_S = mu * results[i]['S']
        death_E = mu * results[i]['E']
        death_I = (mu + alpha) * results[i]['I']
        death_R = mu * results[i]['R']
        latency = sigma * results[i]['E']
        recovery = gamma * results[i]['I']

        S_rate = birth - infection + lost_immunity - death_S
        E_rate = infection - latency - death_E
        I_rate = latency - recovery - death_I
        R_rate = recovery - lost_immunity - death_R

        row = i + 1
        results[row] = {
            'time': round(row * time_unit_years, 3),
            'S': results[i]['S'] + S_rate,
            'E': results[i]['E'] + E_rate,
            'I': results[i]['I'] + I_rate,
            'R': results[i]['R'] + R_rate,
        }

    return results

def write_model_to_csv(model_output: dict, path: str):
    """
    Write the model results to a given csv file

    Args:
        |   model_output: dict
        |   path: path to write csv to including .csv extension
    """
    with open(path, 'w', newline='') as outfile:
        dictWriter = csv.DictWriter(
        outfile,
        fieldnames=model_output[0].keys(),
        delimiter=',',
        quoting=csv.QUOTE_NONNUMERIC
        )
        dictWriter.writeheader()
        for i in model_output:
            dictWriter.writerow(model_output[i])
        logging.info('Success file: {} written'.format(path))

def getResourceDirectory():
    """
    Returns the path of the resouce directory containing intial parameters etc.

    Returns:
        |   str: path to the resource directory
    """
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "ext")

def readInitialParameters(path = ""):
    """
    Read the inital parameters from a csv into a dictionary

    Args:
        |   path: (optional) path to csv file

    Returns:
        |   dict: containing the intial parameters
    """
    if path == "":
        path = os.path.join(getResourceDirectory(), "static_params_SEIRS.csv")
    rtn = {}
    with open(path, 'r', newline='') as data:
        reader = csv.DictReader(data)
        for line in reader:
            rtn[line['param']] = float(line['value'])
    return rtn