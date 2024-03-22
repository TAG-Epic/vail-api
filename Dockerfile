FROM python:3.11
RUN pip install poetry
WORKDIR /app
COPY pyproject.toml ./
RUN mkdir vail_scraper && touch vail_scraper/__init__.py && touch README.md
RUN poetry install
RUN rm -rf vail_scraper
COPY . ./
CMD ["poetry", "run", "python3", "-m", "vail_scraper"]

