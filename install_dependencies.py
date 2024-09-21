import os
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import pip
except ImportError:
    print("pip não está instalado. Instalando pip...")
    subprocess.check_call([sys.executable, "-m", "ensurepip", "--upgrade"])

def install_requirements():
    if os.path.exists("requirements.txt"):
        print("Instalando dependências do arquivo requirements.txt...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
    else:
        print("Arquivo requirements.txt não encontrado!")

if __name__ == "__main__":
    install_requirements()
