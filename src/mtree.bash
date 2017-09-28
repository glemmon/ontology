#!/usr/bin/env bash

for label in OD CO DOID GO HP MP
do
	./mtree.py $label > trees/${label}.tree
	./mtree.py --pad $label > trees/${label}.pad.tree
done

for label in CHEBI ICD9dx ICD10dx
do
	./mtree.py $label > trees/${label}.tree
done
