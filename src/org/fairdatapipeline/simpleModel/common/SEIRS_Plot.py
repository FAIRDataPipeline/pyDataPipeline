import matplotlib.pyplot as plt

def SEIRS_Plot(results: dict, save_location: str, show = False):
    """
    Produces plot from results of SEIRS Model and saves the plot as a png
        in the save_location

    Args:
        results: results from SEIRS Model
        save_location: location to save plot
        show: whether or not to show the plot as well as saving
    """
    time_points = [round(results[k]['Time'] / 365.25, 3) for k in results]
    S = [results[k]['S'] * 100 for k in results]
    E = [results[k]['E'] * 100 for k in results]
    I = [results[k]['I'] * 100 for k in results]
    R = [results[k]['R'] * 100 for k in results]
    #print(time_points)
    t1 = "\nR\u2080=3, 1/\u03B3=14 Days, 1/\u03C3=7 Days, 1/\u03C9=1 Year"
    plt.plot(time_points, S, label = "S")
    plt.plot(time_points, E, label = "E")
    plt.plot(time_points, I, label = "I")
    plt.plot(time_points, R, label = "R")
    plt.xlabel("Years")
    plt.ylabel("Relative Group Size (%)")
    plt.title("SEIRS Model Trajectories" + t1)
    plt.legend()
    plt.savefig(save_location)
    if show:
        plt.show()