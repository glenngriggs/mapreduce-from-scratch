"""Example threading."""
import time
import threading


def main():
    """Test Threading."""
    print("main() starting")
    threads = []
    thread = threading.Thread(target=worker)
    threads.append(thread)
    thread.start()
    print("main() can do other work here")
    # ...
    print("main() waiting for worker() to exit")
    thread.join()
    print("main() shutting down")


def worker():
    """Worker thread."""
    print("worker() starting")
    time.sleep(10)
    print("worker() shutting down")


if __name__ == "__main__":
    main()
