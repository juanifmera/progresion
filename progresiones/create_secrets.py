# create_secrets.py
import secrets as _secrets_lib
from pathlib import Path
import toml
import getpass

# Intentamos usar streamlit_authenticator Hasher si está disponible
try:
    import streamlit_authenticator as stauth
except Exception:
    stauth = None

# Fallback a passlib (bcrypt) si la API de Hasher no funciona
try:
    from passlib.hash import bcrypt
except Exception:
    bcrypt = None


def hash_password(password: str) -> str:
    """
    Devuelve un hash compatible para usar en streamlit_authenticator.
    Primero intenta usar stauth.Hasher con ambas firmas:
      - stauth.Hasher([password]).generate()[0]
      - stauth.Hasher().generate([password])[0]
    Si falla, usa passlib bcrypt (recomendado instalar passlib[bcrypt]).
    """
    # Intento con streamlit_authenticator si está instalado
    if stauth is not None:
        try:
            # Primer intento: la vieja forma (algunas versiones)
            return stauth.Hasher([password]).generate()[0]
        except TypeError:
            try:
                # Segunda forma: instanciar sin args y pasar la lista a generate
                return stauth.Hasher().generate([password])[0]
            except Exception:
                # seguimos al fallback
                pass
        except Exception:
            pass

    # Fallback: passlib bcrypt
    if bcrypt is not None:
        # rounds 12 por defecto; es compatible con la mayoría de verificadores bcrypt
        return bcrypt.using(rounds=12).hash(password)

    # Si no hay ninguna librería disponible, levantamos un error claro
    raise RuntimeError(
        "No se pudo generar el hash: ni streamlit_authenticator ni passlib están disponibles. "
        "Instalá 'streamlit-authenticator' o 'passlib[bcrypt]' e intentá de nuevo."
    )


def write_secrets_toml(username_key: str,
                       display_name: str,
                       email: str,
                       password_hash: str,
                       cookie_name: str = "streamlit_cookie",
                       cookie_key: str | None = None,
                       expiry_days: int = 30,
                       out_path: str = "secrets.toml"):
    """
    Crea y escribe un archivo secrets.toml con la estructura esperada por streamlit_authenticator.
    username_key: identificador interno (ej. 'admincdg') -> se usará como clave dentro de credentials.usernames
    display_name: nombre a mostrar
    email: email del usuario
    password_hash: contraseña ya hasheada (valor retornado por hash_password)
    """
    if cookie_key is None:
        cookie_key = _secrets_lib.token_urlsafe(32)

    toml_dict = {
        "credentials": {
            "usernames": {
                username_key: {
                    "email": email,
                    "name": display_name,
                    "password": password_hash
                }
            }
        },
        "cookie": {
            "name": cookie_name,
            "key": cookie_key,
            "expiry_days": expiry_days
        }
    }

    out_path = Path(out_path)
    out_path.write_text(toml.dumps(toml_dict), encoding="utf-8")
    print(f"secrets.toml creado en: {out_path.resolve()}")


if __name__ == "__main__":
    # Valores por defecto según pediste
    username_key = "admincdg"               # clave interna (sin @)
    display_name = "Admin CDG"
    email = "admincdg@carrefour.com"

    # Pedimos la password por consola para no dejarla en claro en el script
    plain_password = getpass.getpass(prompt="Ingresá la contraseña para el usuario admincdg@carrefour.com: ")

    # Generar hash
    try:
        pwd_hash = hash_password(plain_password)
    except Exception as e:
        print("Error al hashear la contraseña:", e)
        raise

    # Escribir secrets.toml
    write_secrets_toml(
        username_key,
        display_name,
        email,
        pwd_hash,
        cookie_name="carrefour_toolbox_cookie",
        expiry_days=30,
        out_path="secrets.toml"
    )

    print("Listo. Mové el secrets.toml a la carpeta .streamlit/ o usalo según tu despliegue.")