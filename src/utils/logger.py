import logging
import sys

def get_logger(name):
    """
    Crée un logger standardisé pour tous les scripts du projet.
    Usage: 
        from src.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Message d'information")
    """
    logger = logging.getLogger(name)
    
    # Éviter de dupliquer les logs si le logger existe déjà
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Format : Date Heure - Nom du script - Niveau (INFO/ERROR) - Message
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Log vers la console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger