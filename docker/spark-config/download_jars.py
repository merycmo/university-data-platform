import urllib.request
import os

JARS = [
    (
        "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar",
        "/opt/spark/jars/hudi-spark3.5-bundle_2.12-0.15.0.jar"
    ),
    (
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar"
    ),
    (
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    ),
]

for url, dest in JARS:
    if os.path.exists(dest):
        print(f"✅ Déjà présent : {dest}")
        continue
    print(f"⬇️ Téléchargement : {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"✅ Téléchargé : {dest}")

print("✅ Tous les JARs sont prêts !")