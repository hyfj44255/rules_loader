#!/bin/bash

INIFILE=$1
SECTION=$2
ITEM=$3
NEWVAL=$4

function ReadINIfile()
{
	ReadINI=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
	echo $ReadINI
}

function WriteINIfile()
{
	# WriteINI=`sed -i"/^\[$SECTION\]/,/^\[/ {/^\[$SECTION\]/b;/^\[/b;s/^$ITEM*=.*/$ITEM=$NEWVAL/g;}" $INIFILE`
	awk -F '=' '/\['${SECTION}'\]/{a=1} (a==1 && "'${ITEM}'"==$1){gsub($2,"'${NEWVAL}'");a=0} {print $0}' ${INIFILE} 1<>${INIFILE}
	echo $WriteINI
}

if [ "$4" = "" ] ;then
	ReadINIfile $1 $2 $3
else
	WriteINIfile $1 $2 $3 $4
fi