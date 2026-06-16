from datetime import date
from io import BytesIO
from pathlib import Path
import tomllib

import pandas as pd
import psycopg2
import streamlit as st

EXCEL_SEED_PATH = Path("socis.xlsx")
BASE_SOCIOS_COLS = ["num", "nom_cognoms"]


def _load_local_secrets() -> dict:
    app_dir = Path(__file__).resolve().parent
    candidates = [
        app_dir / ".streamlit" / "secrets.toml",
        app_dir / "streamlit" / "secrets.toml",
        Path.cwd() / ".streamlit" / "secrets.toml",
        Path.cwd() / "streamlit" / "secrets.toml",
    ]
    for path in candidates:
        if path.exists():
            # utf-8-sig strips BOM if present (common when files are saved from Windows editors)
            return tomllib.loads(path.read_text(encoding="utf-8-sig"))
    return {}


def _db_config() -> dict:
    try:
        secrets_obj = dict(st.secrets)
    except Exception:
        secrets_obj = {}

    # Local fallback for desktop runs when Streamlit doesn't expose st.secrets properly.
    local_secrets = _load_local_secrets()
    for key, value in local_secrets.items():
        secrets_obj.setdefault(key, value)
    if "DATABASE_URL" in secrets_obj:
        return {"dsn": str(secrets_obj["DATABASE_URL"])}

    has_postgres = "postgres" in secrets_obj

    if has_postgres:
        sec = secrets_obj["postgres"]
        return {
            "host": sec["host"],
            "port": int(sec.get("port", 5432)),
            "dbname": sec["dbname"],
            "user": sec["user"],
            "password": sec["password"],
            "sslmode": sec.get("sslmode", "require"),
        }

    required = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [k for k in required if k not in secrets_obj]
    if missing:
        raise ValueError(
            "Falten secrets de BD. Usa `DATABASE_URL` (recomanat) o [postgres]/claus DB_ a `.streamlit/secrets.toml`."
        )

    return {
        "host": secrets_obj["DB_HOST"],
        "port": int(secrets_obj.get("DB_PORT", 5432)),
        "dbname": secrets_obj["DB_NAME"],
        "user": secrets_obj["DB_USER"],
        "password": secrets_obj["DB_PASSWORD"],
        "sslmode": secrets_obj.get("DB_SSLMODE", "require"),
    }


def get_conn():
    cfg = _db_config()
    if "dsn" in cfg:
        return psycopg2.connect(cfg["dsn"])
    return psycopg2.connect(**cfg)


def _normalizar_estado(valor: object) -> str | None:
    if pd.isna(valor):
        return None
    texto = str(valor).strip().upper()
    if texto in {"S", "SI", "1", "TRUE", "T", "X"}:
        return "1"
    if texto in {"N", "NO", "0", "FALSE", "F", ""}:
        return None
    return None


def _es_columna_fecha(col: str) -> bool:
    try:
        pd.to_datetime(col, format="%Y-%m-%d")
        return True
    except Exception:
        return False


def _quote_ident(ident: str) -> str:
    escaped = ident.replace('"', '""')
    return f'"{escaped}"'


def _table_exists(table_name: str) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return cur.fetchone() is not None


def _query_df(query: str, params: tuple | None = None) -> pd.DataFrame:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, params or ())
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=300, show_spinner=False)
def columnas_socios() -> list[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'socios'
            ORDER BY ordinal_position
            """
        )
        rows = cur.fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300, show_spinner=False)
def columnas_fecha_socios() -> list[str]:
    return sorted([c for c in columnas_socios() if c not in BASE_SOCIOS_COLS and _es_columna_fecha(c)])


def asegurar_columna_fecha(fecha: str) -> bool:
    cols = columnas_socios()
    if fecha in cols:
        return False

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE public.socios ADD COLUMN {_quote_ident(fecha)} TEXT")

    invalidar_cache_datos()
    return True


def _inicializar_db_impl() -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.socios (
                num INTEGER PRIMARY KEY,
                nom_cognoms TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute("SELECT COUNT(*) FROM public.socios")
        total_socios = cur.fetchone()[0]

    if total_socios == 0 and EXCEL_SEED_PATH.exists():
        cargar_excel_inicial_en_db(EXCEL_SEED_PATH)

    migrar_asistencias_antiguas()


@st.cache_resource(show_spinner=False)
def inicializar_db() -> bool:
    _inicializar_db_impl()
    return True


def migrar_asistencias_antiguas() -> None:
    if not _table_exists("asistencias"):
        return

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT num, fecha, estado FROM public.asistencias")
        filas = cur.fetchall()
        if not filas:
            return

        fechas = sorted({str(f[1]) for f in filas if _es_columna_fecha(str(f[1]))})
        actuales = set(columnas_socios())
        for fecha in fechas:
            if fecha not in actuales:
                cur.execute(f"ALTER TABLE public.socios ADD COLUMN {_quote_ident(fecha)} TEXT")

        for num, fecha, estado in filas:
            estado_norm = _normalizar_estado(estado)
            if not estado_norm or not _es_columna_fecha(str(fecha)):
                continue
            cur.execute(
                f"UPDATE public.socios SET {_quote_ident(str(fecha))} = %s WHERE num = %s",
                (estado_norm, int(num)),
            )

        cur.execute("DROP TABLE IF EXISTS public.asistencias")

    invalidar_cache_datos()


def cargar_excel_inicial_en_db(path: Path) -> None:
    df = pd.read_excel(path, engine="openpyxl")

    if "NÚM" not in df.columns:
        raise ValueError("Falta la columna obligatoria 'NÚM' en el Excel inicial.")

    numeros = pd.to_numeric(df["NÚM"], errors="coerce")
    if numeros.isna().any():
        raise ValueError("La columna 'NÚM' del Excel inicial contiene valores no válidos.")

    df["NÚM"] = numeros.astype(int)

    if "NOM I COGNOMS" not in df.columns:
        df["NOM I COGNOMS"] = ""

    if df["NÚM"].duplicated().any():
        raise ValueError("El Excel inicial contiene números de socio duplicados.")

    columnas_fecha = [str(c) for c in df.columns if _es_columna_fecha(str(c))]

    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO public.socios (num, nom_cognoms)
            VALUES (%s, %s)
            ON CONFLICT(num) DO UPDATE SET nom_cognoms = EXCLUDED.nom_cognoms
            """,
            [
                (int(row["NÚM"]), "" if pd.isna(row["NOM I COGNOMS"]) else str(row["NOM I COGNOMS"]))
                for _, row in df.iterrows()
            ],
        )

        actuales = set(columnas_socios())
        for fecha in columnas_fecha:
            if fecha not in actuales:
                cur.execute(f"ALTER TABLE public.socios ADD COLUMN {_quote_ident(fecha)} TEXT")

        for _, row in df.iterrows():
            num = int(row["NÚM"])
            for fecha in columnas_fecha:
                estado = _normalizar_estado(row[fecha])
                if estado:
                    cur.execute(
                        f"UPDATE public.socios SET {_quote_ident(fecha)} = %s WHERE num = %s",
                        (estado, num),
                    )

    invalidar_cache_datos()


@st.cache_data(ttl=300, show_spinner=False)
def cargar_socios_base() -> pd.DataFrame:
    df = _query_df("SELECT num AS \"NÚM\", nom_cognoms AS \"NOM I COGNOMS\" FROM public.socios ORDER BY num")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def proximo_num_socio() -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(num), 0) + 1 FROM public.socios")
        return int(cur.fetchone()[0])


def crear_socio(nom_cognoms: str) -> int:
    nombre = nom_cognoms.strip()
    if not nombre:
        raise ValueError("El nom i cognoms no pot estar buit.")

    with get_conn() as conn, conn.cursor() as cur:
        # Bloquegem la taula per evitar col·lisions si dos altes arriben alhora.
        cur.execute("LOCK TABLE public.socios IN EXCLUSIVE MODE")
        cur.execute("SELECT COALESCE(MAX(num), 0) + 1 FROM public.socios")
        nuevo_num = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO public.socios (num, nom_cognoms)
            VALUES (%s, %s)
            """,
            (nuevo_num, nombre),
        )

    invalidar_cache_datos()
    return nuevo_num


def existe_fecha(fecha: str) -> bool:
    return fecha in columnas_socios()


def guardar_asistencia_db(presentes: set[int], quitar_presentes: set[int], fecha: str) -> bool:
    sobrescribe = existe_fecha(fecha)
    asegurar_columna_fecha(fecha)

    with get_conn() as conn, conn.cursor() as cur:
        if presentes:
            cur.executemany(
                f"UPDATE public.socios SET {_quote_ident(fecha)} = '1' WHERE num = %s",
                [(num,) for num in presentes],
            )
        if quitar_presentes:
            cur.executemany(
                f"UPDATE public.socios SET {_quote_ident(fecha)} = NULL WHERE num = %s",
                [(num,) for num in quitar_presentes],
            )

    invalidar_cache_datos()
    return sobrescribe


def construir_excel_asistencia_dia(fecha: str) -> pd.DataFrame:
    if existe_fecha(fecha):
        df = _query_df(
            f"SELECT num, nom_cognoms, {_quote_ident(fecha)} AS {_quote_ident(fecha)} FROM public.socios ORDER BY num"
        )
    else:
        df = _query_df(
            f"SELECT num, nom_cognoms, NULL::TEXT AS {_quote_ident(fecha)} FROM public.socios ORDER BY num"
        )

    rename_map = {"num": "NÚM", "nom_cognoms": "NOM I COGNOMS"}
    return df.rename(columns=rename_map)


def limpiar_historico_asistencias() -> int:
    fechas = columnas_fecha_socios()
    if not fechas:
        return 0

    with get_conn() as conn, conn.cursor() as cur:
        for fecha in fechas:
            cur.execute(f"ALTER TABLE public.socios DROP COLUMN IF EXISTS {_quote_ident(fecha)}")

    invalidar_cache_datos()
    return len(fechas)


def invalidar_cache_datos() -> None:
    st.cache_data.clear()


def dataframe_a_excel_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer.read()


def main() -> None:
    st.set_page_config(page_title="Control d'Asistència - Xafacamins", layout="wide")
    st.title("Control d'Assistència - Xafacamins")
    st.caption("Base de dades PostgreSQL remota (Supabase/Neon), consultable des de DBeaver")

    if "presentes" not in st.session_state:
        st.session_state.presentes = set()
    if "quitar_presentes" not in st.session_state:
        st.session_state.quitar_presentes = set()
    if "excel_export" not in st.session_state:
        st.session_state.excel_export = None
    if "mensaje_add" not in st.session_state:
        st.session_state.mensaje_add = None
    if "num_add" not in st.session_state:
        st.session_state.num_add = None

    try:
        inicializar_db()
        df_socios = cargar_socios_base()
    except Exception as e:
        st.error(f"Error inicialitzant la base de dades: {e}")
        st.stop()

    socios_ids = set(df_socios["NÚM"].tolist()) if not df_socios.empty else set()
    nombre_por_id = (
        {
            int(row["NÚM"]): str(row["NOM I COGNOMS"]) if pd.notna(row["NOM I COGNOMS"]) else ""
            for _, row in df_socios[["NÚM", "NOM I COGNOMS"]].iterrows()
        }
        if not df_socios.empty
        else {}
    )
    if st.session_state.num_add not in socios_ids:
        st.session_state.num_add = None

    fecha_entreno = st.date_input("Data de l'entrenament", value=date.today(), format="YYYY-MM-DD")
    fecha_columna = fecha_entreno.strftime("%Y-%m-%d")

    if existe_fecha(fecha_columna):
        st.warning(
            f"La columna {fecha_columna} ja existeix: s'actualitzaran només els socis que modifiquis en aquesta sessió."
        )

    st.subheader("Passar llista per número de soci")

    c1, c2 = st.columns(2)

    def _numero_add_actual() -> int | None:
        numero = st.session_state.get("num_add")
        if numero is None:
            return None
        return int(numero)

    def _afegir_soci_actual() -> None:
        numero = _numero_add_actual()
        if numero is None:
            st.session_state.mensaje_add = ("warning", "Introdueix un número de soci vàlid.")
            return
        if numero not in socios_ids:
            st.session_state.mensaje_add = ("warning", f"El soci {numero} no existeix a la base de dades.")
            return

        st.session_state.presentes.add(numero)
        st.session_state.quitar_presentes.discard(numero)
        st.session_state.mensaje_add = ("success", f"Soci {numero} afegit a l'assistència del dia.")

    with c1:
        opciones_socios = sorted(socios_ids)
        st.selectbox(
            "NÚM per Afegir",
            options=opciones_socios,
            index=None,
            format_func=lambda numero: f"{numero} - {nombre_por_id.get(numero, '')}",
            placeholder="Escriu el número de soci",
            key="num_add",
            on_change=_afegir_soci_actual,
        )
        numero_add = _numero_add_actual()

        if numero_add is not None and numero_add in socios_ids:
            nom_add = nombre_por_id.get(numero_add, "")
            st.info(f"**{numero_add} - {nom_add}**")
        elif numero_add is not None:
            st.warning(f"El soci {numero_add} no existeix a la base de dades.")

        st.button("Afegir a assistència del dia", key="btn_confirm_add", on_click=_afegir_soci_actual)
        if st.session_state.mensaje_add:
            tipo_mensaje, texto_mensaje = st.session_state.mensaje_add
            if tipo_mensaje == "success":
                st.success(texto_mensaje)
            else:
                st.warning(texto_mensaje)

    with c2:
        with st.form("form_quitar", clear_on_submit=True):
            numero_input_del = st.number_input(
                "NÚM per TREURE",
                min_value=1,
                step=1,
                format="%d",
                key="num_del",
            )
            enviar_del = st.form_submit_button("Treure")

        if enviar_del:
            numero = int(numero_input_del)
            if numero not in socios_ids:
                st.warning(f"El soci {numero} no existeix a la base de dades.")
            else:
                st.session_state.quitar_presentes.add(numero)
                st.session_state.presentes.discard(numero)
                st.success(f"Soci {numero} marcat per TREURE.")

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Assistència del dia")
        add_ordenados = sorted(st.session_state.presentes)
        st.metric("Total assistència", len(add_ordenados))
        if add_ordenados:
            h1, h2, h3 = st.columns([1, 3, 1])
            h1.markdown("**NÚM**")
            h2.markdown("**NOM I COGNOMS**")
            h3.markdown("**Acció**")

            for numero in add_ordenados:
                cnum, cnom, cbtn = st.columns([1, 3, 1])
                cnum.write(numero)
                cnom.write(nombre_por_id.get(numero, ""))
                if cbtn.button("Eliminar", key=f"btn_del_add_{numero}"):
                    st.session_state.presentes.discard(numero)
                    st.success(f"Soci {numero} eliminat de l'assistència del dia.")
                    st.rerun()

    with col_b:
        st.subheader("Pendents per treure")
        del_ordenados = sorted(st.session_state.quitar_presentes)
        st.metric("Total per treure", len(del_ordenados))
        if del_ordenados:
            st.dataframe(
                pd.DataFrame(
                    {"NÚM": del_ordenados, "NOM I COGNOMS": [nombre_por_id.get(n, "") for n in del_ordenados]}
                ),
                use_container_width=True,
                hide_index=True,
            )

    cclear1, cclear2 = st.columns(2)
    with cclear1:
        if st.button("Netejar pendents de afegir"):
            st.session_state.presentes = set()
            st.rerun()
    with cclear2:
        if st.button("Netejar pendents de treure"):
            st.session_state.quitar_presentes = set()
            st.rerun()

    st.divider()

    csave, cexport = st.columns(2)
    with csave:
        if st.button("Desar assistència", type="primary"):
            if not st.session_state.presentes and not st.session_state.quitar_presentes:
                st.info("No hi ha canvis pendents per desar.")
            else:
                try:
                    sobrescribe = guardar_asistencia_db(
                        presentes=st.session_state.presentes,
                        quitar_presentes=st.session_state.quitar_presentes,
                        fecha=fecha_columna,
                    )
                    if sobrescribe:
                        st.warning(f"S'ha actualitzat la columna existent {fecha_columna}.")
                    st.success("Canvis desats a PostgreSQL.")

                    st.session_state.presentes = set()
                    st.session_state.quitar_presentes = set()
                    st.session_state.excel_export = None
                    st.rerun()
                except Exception as e:
                    st.error(f"No s'ha pogut desar l'assistència: {e}")

    with cexport:
        if st.button("Preparar Excel d'assistència", use_container_width=True):
            try:
                df_export = construir_excel_asistencia_dia(fecha_columna)
                excel_bytes = dataframe_a_excel_bytes(df_export)
                st.session_state.excel_export = {
                    "fecha": fecha_columna,
                    "bytes": excel_bytes,
                    "file_name": f"assistencia_{fecha_columna}.xlsx",
                }
            except Exception as e:
                st.session_state.excel_export = None
                st.error(f"No s'ha pogut preparar l'Excel d'exportació: {e}")

        export_actual = st.session_state.excel_export
        if export_actual and export_actual["fecha"] == fecha_columna:
            st.download_button(
                "Descarregar assistència del dia (Excel)",
                data=export_actual["bytes"],
                file_name=export_actual["file_name"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        elif export_actual:
            st.info("Prepara l'Excel per a la data seleccionada abans de descarregar-lo.")

    st.divider()
    st.subheader("Alta de nou soci")
    try:
        siguiente_num = proximo_num_socio()
        st.caption(f"El següent número disponible és el {siguiente_num}.")
    except Exception as e:
        st.warning(f"No s'ha pogut calcular el següent número: {e}")

    with st.form("form_alta_socio", clear_on_submit=True):
        nom_nou = st.text_input("Nom i cognoms", placeholder="Ex. Joan Pérez")
        alta_enviada = st.form_submit_button("Donar d'alta")

    if alta_enviada:
        try:
            nuevo_num = crear_socio(nom_nou)
            st.success(f"Soci donat d'alta amb el número {nuevo_num}.")
            st.session_state.excel_export = None
            st.rerun()
        except Exception as e:
            st.error(f"No s'ha pogut donar d'alta el soci: {e}")

    st.divider()
    st.subheader("Administració")
    st.warning("Aquesta acció elimina tot l'històric d'assistències (totes les dates) i no es pot desfer.")
    confirmar_limpieza = st.checkbox("Confirmo que vull eliminar tot l'històric", value=False)
    if st.button("Eliminar històric complet", type="secondary"):
        if not confirmar_limpieza:
            st.error("Marca la confirmació abans d'eliminar l'històric.")
        else:
            try:
                total = limpiar_historico_asistencias()
                st.session_state.presentes = set()
                st.session_state.quitar_presentes = set()
                st.session_state.excel_export = None
                if total == 0:
                    st.info("No hi havia cap columna d'històric per eliminar.")
                else:
                    st.success(f"S'han eliminat {total} columnes d'històric.")
                st.rerun()
            except Exception as e:
                st.error(f"No s'ha pogut eliminar l'històric: {e}")


if __name__ == "__main__":
    main()

