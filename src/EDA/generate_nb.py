import nbformat as nbf
import os

def create_eda_notebook():
    nb = nbf.v4.new_notebook()

    cells = []

    # Title and Introduction
    cells.append(nbf.v4.new_markdown_cell("# Basic Exploratory Data Analysis: MOBIE Datasets\n"
                                         "This notebook performs structural analysis and general distribution plotting for the Portuguese EV charging station datasets."))

    # Imports
    cells.append(nbf.v4.new_code_cell("import pandas as pd\n"
                                     "import matplotlib.pyplot as plt\n"
                                     "import seaborn as sns\n"
                                     "import os\n"
                                     "\n"
                                     "# Set plot style\n"
                                     "sns.set_theme(style='whitegrid')\n"
                                     "plt.rcParams['figure.figsize'] = (12, 6)"))

    # Data Path Configuration
    cells.append(nbf.v4.new_code_cell("DATA_DIR = '../../data'\n"
                                     "POSTOS_FILE = os.path.join(DATA_DIR, 'MOBIe_Lista_de_postos_corrected.csv')\n"
                                     "TARIFAS_FILE = os.path.join(DATA_DIR, 'MOBIE_Tarifas.csv')"))

    # Loading Datasets
    cells.append(nbf.v4.new_code_cell("# Load datasets\n"
                                     "df_postos = pd.read_csv(POSTOS_FILE, sep=';', encoding='utf-8-sig')\n"
                                     "df_tarifas = pd.read_csv(TARIFAS_FILE, sep=';', encoding='utf-8-sig')\n"
                                     "\n"
                                     "print('Datasets loaded successfully.')"))

    # Structural Analysis Helper
    cells.append(nbf.v4.new_code_cell("def print_structure(df, name):\n"
                                     "    print(f'\\n--- Structure of {name} ---')\n"
                                     "    print(f'Shape: {df.shape}')\n"
                                     "    print('\\nColumn Info:')\n"
                                     "    print(df.info())\n"
                                     "    print('\\nFirst 5 rows:')\n"
                                     "    display(df.head())\n"
                                     "\n"
                                     "print_structure(df_postos, 'MOBIe_Lista_de_postos')\n"
                                     "print_structure(df_tarifas, 'MOBIE_Tarifas')"))

    # Distribution Plotting Helper
    cells.append(nbf.v4.new_code_cell("import numpy as np\n"
                                     "\n"
                                     "SOCKET_LEVEL_COLS = [\n"
                                     "    'POTENCIA_TOMADA', 'FORMATO_TOMADA', 'TIPO_TOMADA', 'NÍVEL DE TENSÃO',\n"
                                     "    'TIPO_TARIFARIO', 'NIVELTENSAO', 'OPERADOR', 'MORADA', 'MUNICIPIO', \n"
                                     "    'TIPO_POSTO', 'UID_TOMADA', 'UID DA TOMADA', 'POTÊNCIA DA TOMADA (kW)',\n"
                                     "    'TIPO DE TOMADA', 'FORMATO DA TOMADA', 'CIDADE', 'ESTADO DO POSTO'\n"
                                     "]\n"
                                     "\n"
                                     "COLS_TO_EXCLUDE = [\n"
                                     "    'ID', 'UID_TOMADA', 'UID DA TOMADA', 'MORADA', 'MOBICHARGER', \n"
                                     "    'MOBICARGA', 'ULTIMA ATUALIZAÇÃO', 'Última Atualização'\n"
                                     "]\n"
                                     "\n"
                                     "def plot_distributions(df, df_name):\n"
                                     "    print(f'\\nProcessing distributions for {df_name}...')\n"
                                     "    for col in df.columns:\n"
                                     "        if any(excl.lower() in col.lower() for excl in COLS_TO_EXCLUDE):\n"
                                     "            continue\n"
                                     "            \n"
                                     "        # Determine deduplication column\n"
                                     "        dedup_col = None\n"
                                     "        if 'UID_TOMADA' in df.columns:\n"
                                     "            dedup_col = 'UID_TOMADA'\n"
                                     "        elif 'UID DA TOMADA' in df.columns:\n"
                                     "            dedup_col = 'UID DA TOMADA'\n"
                                     "            \n"
                                     "        # Use deduplicated dataframe for socket-level columns\n"
                                     "        plot_df = df\n"
                                     "        if dedup_col and col in SOCKET_LEVEL_COLS:\n"
                                     "            plot_df = df.drop_duplicates(subset=dedup_col)\n"
                                     "            print(f'  Deduplicating {col} by {dedup_col} ({len(plot_df)} unique entries)')\n"
                                     "        \n"
                                     "        # Determine if categorical or numerical\n"
                                     "        if plot_df[col].dtype == 'object' or plot_df[col].nunique() < 20:\n"
                                     "            # Categorical logic\n"
                                     "            counts = plot_df[col].value_counts()\n"
                                     "            \n"
                                     "            # Special handling for OPERADOR: Group small counts and show all\n"
                                     "            if 'OPERADOR' in col.upper():\n"
                                     "                others_mask = counts < 10\n"
                                     "                if others_mask.any():\n"
                                     "                    others_count = counts[others_mask].sum()\n"
                                     "                    counts = counts[~others_mask]\n"
                                     "                    counts['Others'] = others_count\n"
                                     "                \n"
                                     "                order = counts.index\n"
                                     "                plt.figure(figsize=(10, max(5, len(counts) * 0.3)))\n"
                                     "                ax = sns.countplot(data=plot_df[plot_df[col].isin(counts.index.drop('Others', errors='ignore')) | (plot_df[col].isna() == False)], \n"
                                     "                                   y=col, order=order, palette='viridis')\n"
                                     "                # Manually adjust 'Others' in the plot is tricky with countplot, simpler to use barplot for frequencies\n"
                                     "                plt.clf()\n"
                                     "                plt.figure(figsize=(10, max(5, len(counts) * 0.3)))\n"
                                     "                sns.barplot(x=counts.values, y=counts.index, palette='viridis')\n"
                                     "                plt.title(f'Distribution: {col} (Grouped <10)')\n"
                                     "                plt.xlabel('Count / Frequency')\n"
                                     "                \n"
                                     "                # Add more grid lines (every 100) ONLY for OPERADOR\n"
                                     "                max_val = counts.max()\n"
                                     "                plt.xticks(np.arange(0, max_val + 101, 100))\n"
                                     "            \n"
                                     "            # Special handling for POTENCIA: Power on X axis\n"
                                     "            elif 'POTENCIA' in col.upper() or 'POTÊNCIA' in col.upper():\n"
                                     "                plt.figure(figsize=(12, 6))\n"
                                     "                # Sort numerically\n"
                                     "                try:\n"
                                     "                    order = sorted(plot_df[col].dropna().unique(), key=lambda x: float(str(x).replace(',', '.').split()[0]))\n"
                                     "                except:\n"
                                     "                    order = sorted(plot_df[col].dropna().unique())\n"
                                     "                sns.countplot(data=plot_df, x=col, order=order, palette='magma')\n"
                                     "                plt.title(f'Distribution: {col}')\n"
                                     "                plt.ylabel('Count / Frequency')\n"
                                     "                plt.xticks(rotation=45)\n"
                                     "            \n"
                                     "            else:\n"
                                     "                plt.figure(figsize=(10, 5))\n"
                                     "                order = counts.index[:15]\n"
                                     "                sns.countplot(data=plot_df, y=col, order=order, palette='viridis')\n"
                                     "                plt.title(f'Top Categories Distribution: {col}')\n"
                                     "                plt.xlabel('Count / Frequency')\n"
                                     "                \n"
                                     "        else:\n"
                                     "            # Numerical Plot\n"
                                     "            plt.figure(figsize=(10, 5))\n"
                                     "            try:\n"
                                     "                sns.histplot(plot_df[col].dropna(), kde=True, color='teal')\n"
                                     "                plt.title(f'Numerical Distribution: {col}')\n"
                                     "                plt.xlabel(col)\n"
                                     "                plt.ylabel('Frequency')\n"
                                     "            except Exception as e:\n"
                                     "                print(f'Could not plot numerical distribution for {col}: {e}')\n"
                                     "                plt.close()\n"
                                     "                continue\n"
                                     "        \n"
                                     "        plt.tight_layout()\n"
                                     "        plt.show()") )

    # Execute Plotting
    cells.append(nbf.v4.new_code_cell("# Plot distributions for both datasets\n"
                                     "plot_distributions(df_postos, 'MOBIe_Lista_de_postos')"))

    cells.append(nbf.v4.new_code_cell("plot_distributions(df_tarifas, 'MOBIE_Tarifas')"))

    nb['cells'] = cells

    with open('eda_basic.ipynb', 'w') as f:
        nbf.write(nb, f)

if __name__ == '__main__':
    create_eda_notebook()
