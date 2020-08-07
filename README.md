# OpenStack Scripts for XDMoD cloud usage reporting

This repository contains scripts and patches for OpenStack that help to enable
the reporting of various usage metrics for the XDMoD software tool.

## Event Reporting

The `event_reporting` folder contains a set of patches and a script that will
create a properly formatted JSON file for ingestion by XDMoD.

## Hypervisor Facts

The `hypervisor_fact_reporting` folder includes a python script that outputs the
number of cpus, memory and hostname of each hypervisor in your OpenStack system.
