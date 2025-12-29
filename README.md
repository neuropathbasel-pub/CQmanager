# Table of contents

- [Disclaimer](#Disclaimer)

- [CQmanager installation](#cqmanager-installation)
    - [Python installation](#python-installation)
    - [Install CQmanager with uv](#install-cqmanager-with-uv)
    - [Install CQmanager with pip](#install-cqmanager-with-pip)
- [Running CQmanager](#running-cqcase)
    - [Running CQmanager installed with uv or pip](#running-cqmanager-installed-with-uv-or-pip)
    - [Running CQcase with Docker compose](#running-cqmanager-with-docker-compose)
- [CQmanager functionality](#cqmanager-functionality)


# Disclaimer

Implementation of this software in a diagnostic setting occurs in the sole responsibility of the treating physician.
Usage of this software occurs at the risk of the user. The authors may not be held liable for any damage (including hardware) this software might cause.
Use is explicitly restricted to academic and non-for-profit organizations.

# CQmanager installation

## Python installation

Install [Python3.11](https://www.python.org/downloads/release/python-3110/) for your platform (works also with python 3.10 and 3.12).
You can find installation guide [here](https://docs.python.org/3/using/unix.html) or [here](https://docs.python-guide.org/starting/install3/linux/) or [here](https://phoenixnap.com/kb/how-to-install-python-3-ubuntu).

The installations depend also on git, which you can install in your terminal (on Ubuntu), if missing, as follows:

``` bash
sudo apt update && sudo apt install git-all
```

## Install CQmanager with uv

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/) if you did not do it yet.
2. Navigate to a directory, where you would like to install CnQuant applications 
3. Initiate a new project called, for example cqmanager, and install CQmanager
``` bash
uv init cqmanager && cd cqmanager && uv venv --python 3.11 && uv pip install "git+https://github.com/neuropathbasel-pub/CQmanager"
```

## Install CQmanager with pip

1. Create a directory where you wish to install CQmanager and enter the directory.
2. Create python3.10 or python3.11 or python3.12 virtual environment (you might need to adjust python path below):
``` bash
/usr/bin/python3.11 -m venv .venv
```
or 
``` bash
python3.11 -m venv .venv
```
3. Activate the virtual environment:
``` bash
source .venv/bin/activate
```
4. Install CQmanager:
``` bash
pip install --upgrade pip setuptools \
&& pip install git+https://github.com/neuropathbasel-pub/CQmanager
```

One-liner
``` bash
mkdir cqmanager \
&& cd cqmanager \
&& /usr/bin/python3.11 -m venv .venv \
&& .venv/bin/python3 -m pip install --upgrade pip setuptools \
&& .venv/bin/python3 -m pip install git+https://github.com/neuropathbasel-pub/CQmanager \
&& .venv/bin/python3 -c "import CQmanager" && cd ..
```

# Running CQmanager

>[!NOTE]
>This requires a filled-in .env and a data annotation file, as described in the [CnQuant repository](https://github.com/neuropathbasel-pub/CnQuant).

## Running CQcalc installed with uv or pip

Steps:
1. Enter the directory where you have installed the app.
2. Activate the virtual environment:
``` bash
source .venv/bin/activate
```
3. Run the app in the console:
``` bash
run_cqmanager
```

# CQmanager functionality

CQmanager automates analysis via HTTP POST and GET requests, integrating with custom scripts. It uses Pydantic to reject invalid requests.
CQmanager will download missing CQcalc, CQcase, CQall, and CQall_plotter containers from Docker Hub before starting any of the containers.

>[!WARNING]
>CQcalc and CQmanager require case-sensitive IDAT suffixes _Red.idat and _Grn.idat. Files like _RED.IDAT or _red.idat will not be processed.

>[!NOTE]
>If bin_size, min_probes_per_bin, or preprocessing_method are omitted in POST requests, defaults are used: 50000, 20, and illumina, respectively.

Available endpoints:

1. Queue IDAT pair analysis:
``` bash
curl -X POST "http://localhost:8002/CQmanager/analyse/" \
     -H "Content-Type: application/json" \
     -d '{"sentrix_id": "your_sentrix_id_without_Red.idat_or_Grn.idat_extension", "preprocessing_method": "illumina", "bin_size": 50000, "min_probes_per_bin": 20}'
```
2. Analyze missing IDAT pairs:
``` bash
curl -X POST "http://localhost:8002/CQmanager/analyse_missing/" \
     -H "Content-Type: application/json" \
     -d '{"preprocessing_method": "illumina", "bin_size":50000, "min_probes_per_bin":20}'
```
3. Analyze missing, annotated IDAT pairs for the summary plots
``` bash
curl -X POST "http://localhost:8002/CQmanager/downsize_annotated_samples_for_summary_plots/" \
     -H "Content-Type: application/json" \
     -d '{"preprocessing_method": "illumina", "bin_size":50000, "min_probes_per_bin":20}'
```
4. Start CQall_plotter containers:
``` bash
curl -X POST "http://localhost:8002/CQmanager/make_summary_plots/" \
     -H "Content-Type: application/json" \
     -d '{"preprocessing_method": "illumina", "methylation_classes":"AML,GBM_RTKII", "bin_size":50000, "min_sentrix_ids_per_plot":20}'
```
Or use the defaults:
``` bash
curl -X POST "http://localhost:8002/CQmanager/make_summary_plots/" \
     -H "Content-Type: application/json" \
     -d '{}'
```
5. Check app status:
``` bash
curl "http://localhost:8002/CQmanager/app_status/"
```
6. Check queue status:
``` bash
curl "http://localhost:8002/CQmanager/queue_status/"
```
7. Stop all CQmanager containers:
``` bash
curl "http://localhost:8002/CQmanager/stop_all_cqmanager_analysis_and_plotting_containers/"
```
8. Start CQcase and CQall:
``` bash
curl "http://localhost:8002/CQmanager/start_cqviewers/"
```
9. Stop CQcase and CQall:
``` bash
curl "http://localhost:8002/CQmanager/stop_cqviewers/"
```
10. Clean up non-running containers:
``` bash
curl "http://localhost:8002/CQmanager/containers_cleanup/"
```
11. Check CQcase, CQall, or CnQuant Redis container status:
``` bash
curl "http://localhost:8002/CQmanager/check_cqviewers_containers/"
```
12. Download sample annotations from Google Drive:
``` bash
curl "http://localhost:8002/CQmanager/update_sample_annotations/"
```
13. Download reference annotations from Google Drive:
``` bash
curl "http://localhost:8002/CQmanager/update_reference_annotations/"
```
14. Simulate CQmanager crash (for testing notifications):
``` bash
curl "http://localhost:8002/CQmanager/simulate_crash/"
```


