from fastapi import APIRouter

router = APIRouter(
    prefix="/CQmanager",
)


@router.get(path="/simulate_crash/")
async def simulate_crash():
    """Simulate a server crash by raising an exception.

    Raises:
        Exception: Simulated crash error.
    """
    raise Exception("Simulated crash")
