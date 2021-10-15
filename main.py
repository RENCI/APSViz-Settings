import uvicorn


class App:
    ...


app = App()

if __name__ == "__main__":
    uvicorn.run("src.server:APP", host="127.0.0.1", port=4000, log_level="info", workers=1)
