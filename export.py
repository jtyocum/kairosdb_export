import configparser
import json
import requests
import time


def epoch_ms() -> int:
    """
    Returns current epoch in miliseconds
    """

    return time.time_ns() // 1000000


def set_checkpoint(checkpoint: int):
    """
    Saves the last runtime to a file.
    """

    f = open("export.checkpoint", "w")

    f.write(str(checkpoint))
    f.close()


def get_checkpoint() -> int:
    """
    Returns the last saved runtime.
    """

    f = open("export.checkpoint", "r")

    checkpoint = int(f.read())
    f.close()

    return checkpoint


def kdb_clean_result(result: dict) -> list:
    """
    Cleanup query result: returns metric, tags, and values (including timestamp)
    """

    cleaned = []

    for query in result["queries"]:
        for entry in query["results"]:
            cleaned.append(
                {
                    "metric": entry["name"],
                    "tags": entry["tags"],
                    "values": entry["values"],
                }
            )

    return cleaned


def kdb_query(api_endpoint: str, query: dict) -> dict:
    """
    Submit query to KairosDB: returns result as dict
   """

    r = requests.post(api_endpoint, data=json.dumps(query))

    return r.json()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("export.ini")

    kdb_url = config["DEFAULT"]["kdb_url"]
    metrics = config["DEFAULT"]["metrics"].split(",")
    tags = config["DEFAULT"]["tags"].split(",")

    # query = {
    #     "start_absolute": 1604337145,
    #     "metrics": [
    #         {
    #             "name": "CMv4_1",
    #             "group_by": [
    #                 {
    #                     "name": "tag",
    #                     "tags": [
    #                         "CO",
    #                         "NO",
    #                         "NO2",
    #                         "O3",
    #                         "RH",
    #                         "Temp",
    #                         "Plantower1",
    #                         "Plantower1",
    #                         "S1",
    #                         "S2",
    #                         "sensorCount",
    #                     ],
    #                 }
    #             ],
    #         }
    #     ],
    # }

    # Resume from last checkpoint, or start at beginning of time
    try:
        last_checkpoint = get_checkpoint()
    except:
        last_checkpoint = 0

    # Get start time as epoch
    start_time = epoch_ms()

    # Loop through metrics, and assemble query
    metrics_query = []

    for metric in metrics:
        metrics_query.append(
            {"name": metric, "group_by": [{"name": "tag", "tags": tags,}],}
        )

    query = {
        "start_absolute": last_checkpoint,
        "end_absolute": start_time,
        "metrics": metrics_query,
    }

  
    result = kdb_query(kdb_url, query)

    print(json.dumps(kdb_clean_result(result)))

    # Save checkpoint. Process will start from this point on next run.
    set_checkpoint(start_time)
