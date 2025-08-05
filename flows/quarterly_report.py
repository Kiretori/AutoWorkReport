from tasks.utils import fetch_quarterly_data

if __name__ == "__main__":
    q_data = fetch_quarterly_data(4, 2024)

    for dd, absence, absence_p in q_data:
        print(f"{dd} | {absence} | {absence_p}")
