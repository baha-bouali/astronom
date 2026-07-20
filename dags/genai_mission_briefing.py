"""
## GenAI Mission Briefing DAG

This DAG extends the astronaut data used elsewhere in this project with a
GenAI step: it fetches who is currently in space, then uses the Airflow
Common AI provider (`@task.llm`) to have an LLM write a short, readable
"mission briefing" summarizing the current crew.

Unlike the Astronomer GenAI quickstart, this uses Groq's free API instead of
OpenAI, wired up through Airflow's PydanticAI connection
(`pydanticai_default`). See the `.env` file for how that connection is
configured — swap the `model` field in that connection's `extra` to point at
any other PydanticAI-compatible provider (OpenAI, Anthropic, local Ollama,
etc.) without touching this DAG at all.

For more on the Common AI provider: https://www.astronomer.io/docs/learn/airflow-common-ai-provider
"""

from airflow.sdk import dag, task
from pendulum import datetime, duration
import requests


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "bahabuntu",
        "retries": 3,
        "retry_delay": duration(seconds=5),
    },
    tags=["example", "space", "genai"],
    is_paused_upon_creation=False,
)
def genai_mission_briefing():

    @task
    def get_astronaut_data() -> list[dict]:
        """Fetch who is currently in space. Falls back to sample data if the
        API is unreachable, same pattern as example_astronauts.py."""
        try:
            r = requests.get("http://api.open-notify.org/astros.json", timeout=10)
            r.raise_for_status()
            return r.json()["people"]
        except Exception:
            print("API currently not available, using hardcoded data instead.")
            return [
                {"craft": "ISS", "name": "Marco Alain Sieber"},
                {"craft": "ISS", "name": "Claude Nicollier"},
            ]

    @task
    def build_prompt(people: list[dict]) -> str:
        """Turn the raw astronaut list into a plain-language prompt for the LLM."""
        crew_lines = "\n".join(f"- {p['name']} aboard {p['craft']}" for p in people)
        return (
            f"There are currently {len(people)} people in space:\n"
            f"{crew_lines}\n\n"
            "Write a short, upbeat 3-4 sentence mission briefing summarizing "
            "who is up there and what spacecraft they're on. Keep it factual "
            "and grounded in the list above, no invented details."
        )

    @task.llm(
        llm_conn_id="pydanticai_default",  # set in .env, see DAG docstring
        system_prompt=(
            "You are a mission control communications officer writing brief, "
            "friendly public updates about crewed spaceflight."
        ),
    )
    def generate_mission_briefing(prompt: str) -> str:
        # the string returned here becomes the user prompt sent to the model
        return prompt

    @task
    def print_briefing(briefing: str) -> None:
        print(briefing)

    people = get_astronaut_data()
    prompt = build_prompt(people)
    briefing = generate_mission_briefing(prompt)
    print_briefing(briefing)


genai_mission_briefing()