import os


def get_system_memory_bytes() -> int:
    """Get the total system memory in bytes."""
    page_size: int = os.sysconf("SC_PAGE_SIZE")
    pages: int = os.sysconf("SC_PHYS_PAGES")
    total_memory: int = page_size * pages
    return total_memory


if __name__ == "__main__":
    total_memory_bytes: int = get_system_memory_bytes()
    total_memory_gb: float = total_memory_bytes / (1024**3)
    print(f"Total system memory: {total_memory_bytes} bytes")
    print(f"Total system memory: {total_memory_gb:.2f} GB")
