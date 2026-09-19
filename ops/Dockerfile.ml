FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# requirements-ml.txt is "-r requirements.txt" + xgboost-cpu; both files
# have to be in the build context together for that include to resolve.
COPY requirements.txt requirements-ml.txt ./
RUN pip install --no-cache-dir -r requirements-ml.txt

COPY ml/ ./ml/

# No default CMD to run standalone — this image exists for
# `docker compose run --rm ml python -m ml.eval` / `ml.train`
# (see scripts/retrain_vps.sh), not as a long-running service.
CMD ["python", "-m", "ml.eval"]
