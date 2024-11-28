import keyring
import keyring.util.platform_ as keyring_platform
from keyring.backends.SecretService import Keyring as SecretServiceKeyring

def get_credentials():

    
    print("Keyring config: " + str(keyring_platform.config_root()))
    # Keyring config: /home/lessmann/.config/python_keyring
    
    print("Keyring method: " + str(keyring.get_keyring()))
    # Keyring method: keyrings.alt.file.PlaintextKeyring (priority: 0.5)
    
    # Plaintext ist nicht gut, also habe ich versucht das backend zu ändern auf SecretService
    
    print("Trying to change backend to SecretService")
    # https://pypi.org/project/keyring/#linux 
    # "To specify a keyring backend, set the default-keyring option to the full path of the class for that backend, 
    # such as keyring.backends.macOS.Keyring."
    
    keyring.set_keyring(SecretServiceKeyring())
    
    print("Keyring method: " + str(keyring.get_keyring()))
    
    # Error: Error in py_call_impl(callable, call_args$unnamed, call_args$named) : 
    # RuntimeError: Unable to initialize SecretService: Environment variable DBUS_SESSION_BUS_ADDRESS is unset
    
    # Danach habe ich verschieden Sachen probiert (z.B. DBUS Session zu starten oder die Environment variable zu setzen). Hat leider nicht geklappt.
    # Aber vermutlich mache ich auch etwas falsch, weil ich das DBUS Zeug nicht verstehe.
    
    # Auf https://pypi.org/project/keyring/#linux gibt es noch andere Beispiele für Linux. Zum Beispiel "GNOME Keyring". 
    # Das habe ich auch noch nicht verstanden.

    
    
    # Wenn das mit dem backend klappt kommt dann der Rest mit dem Setzen und Holen von Username und Password.
    service_name = 'ntsportal'
    username = 'ole.lessmann'
    # Retrieve credentials from keyring
    password = keyring.get_password(service_name, username)
    if not password:
      raise ValueError("Password not found in keyring.")
    
    credentials = {
        'username': username,
        'password': password
    }

    return credentials
