"""
=============================================================
 SavePoint — Upload vers AWS S3
 Fichier : ingestion/upload_to_s3.py

 Ce script upload les fichiers Parquet du Data Lake local
 vers un bucket S3, en conservant la structure de partitions.
=============================================================
 Usage :
   pip install boto3 python-dotenv
   python ingestion/upload_to_s3.py
=============================================================
"""

import os
import logging
import boto3
from pathlib import Path
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

# ── Config ──────────────────────────────────────────────────
AWS_ACCESS_KEY    = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY     = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION         = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET          = os.getenv("S3_BUCKET_NAME")

STAGED_DIR         = Path("data/staged")
S3_PREFIX          = "savepoint/staged"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def get_s3_client():
    """Crée un client S3 authentifié."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )


def upload_file(s3_client, local_path: Path, s3_key: str):
    """Upload un fichier vers S3."""
    try:
        s3_client.upload_file(str(local_path), S3_BUCKET, s3_key)
        log.info(f"  ✅ {local_path.name} → s3://{S3_BUCKET}/{s3_key}")
        return True
    except ClientError as e:
        log.error(f"  ❌ Erreur upload {local_path.name} : {e}")
        return False


def upload_directory(s3_client, local_dir: Path, s3_prefix: str):
    """
    Upload récursivement tous les fichiers Parquet d'un dossier,
    en conservant la structure de partitions (continent=/year=).
    """
    if not local_dir.exists():
        log.error(f"Dossier introuvable : {local_dir}")
        return 0, 0

    files = list(local_dir.rglob("*.parquet"))
    log.info(f"Fichiers Parquet trouvés : {len(files)}")

    success, failed = 0, 0
    for filepath in files:
        relative_path = filepath.relative_to(local_dir)
        s3_key = f"{s3_prefix}/{relative_path.as_posix()}"

        if upload_file(s3_client, filepath, s3_key):
            success += 1
        else:
            failed += 1

    return success, failed


def list_bucket_contents(s3_client, prefix: str = ""):
    """Liste le contenu du bucket pour vérification."""
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    objects = response.get("Contents", [])

    print(f"\n{'='*60}")
    print(f"  CONTENU DU BUCKET : s3://{S3_BUCKET}/{prefix}")
    print(f"{'='*60}")

    total_size = 0
    for obj in objects:
        size_kb = obj["Size"] / 1024
        total_size += obj["Size"]
        print(f"  {obj['Key']:<60} {size_kb:>8.1f} KB")

    print(f"{'='*60}")
    print(f"  Total : {len(objects)} fichiers, {total_size/1024/1024:.2f} MB")
    print(f"{'='*60}\n")


def run():
    if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET]):
        raise ValueError(
            "Variables AWS manquantes dans .env : "
            "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME"
        )

    log.info(f"Connexion à S3 (bucket: {S3_BUCKET}, région: {AWS_REGION})...")
    s3_client = get_s3_client()

    # Upload du fichier principal staged
    main_file = Path("data/staged/vgsales_enriched.parquet")
    if main_file.exists():
        log.info("Upload du fichier principal...")
        upload_file(s3_client, main_file, f"{S3_PREFIX}/vgsales_enriched.parquet")

    # Upload des partitions (si elles existent)
    partitioned_dir = STAGED_DIR / "partitioned"
    if partitioned_dir.exists():
        log.info("Upload des partitions...")
        success, failed = upload_directory(s3_client, partitioned_dir, f"{S3_PREFIX}/partitioned")
        log.info(f"Partitions : {success} réussies, {failed} échouées")

    # Vérification finale
    list_bucket_contents(s3_client, S3_PREFIX)

    log.info("✅ Upload S3 terminé !")


if __name__ == "__main__":
    run()