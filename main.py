import contextvars
from flows.dataverse_flow import dataverse_flow
from flows.clockify_flow import clockify_flow
from concurrent.futures import ThreadPoolExecutor

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=5) as executor:
        w1 = executor.submit(contextvars.copy_context().run, clockify_flow)
        w2 = executor.submit(contextvars.copy_context().run, dataverse_flow)

        print(w1.result())
        print(w2.result())