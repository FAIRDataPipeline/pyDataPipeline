import matplotlib.pyplot as plt


def SEIRS_Plot(results: dict, save_location: str, show: bool = False) -> None:
    """
    Produces plot from results of SEIRS Model and saves the plot as a png
        in the save_location

    Args:
        results: results from SEIRS Model
        save_location: location to save plot
        show: whether or not to show the plot as well as saving
    """
    time_points = [results[k]["time"] for k in results]
    S_data = [results[k]["S"] * 100 for k in results]
    E_data = [results[k]["E"] * 100 for k in results]
    I_data = [results[k]["I"] * 100 for k in results]
    R_data = [results[k]["R"] * 100 for k in results]
    t1 = "\nR\u2080=3, 1/\u03B3=14 Days, 1/\u03C3=7 Days, 1/\u03C9=1 Year"
    plt.plot(time_points, S_data, label="S")
    plt.plot(time_points, E_data, label="E")
    plt.plot(time_points, I_data, label="I")
    plt.plot(time_points, R_data, label="R")
    plt.xlabel("Years")
    plt.ylabel("Relative Group Size (%)")
    plt.title("SEIRS Model Trajectories" + t1)
    plt.legend()
    plt.savefig(save_location)
    if show:
        plt.show()
