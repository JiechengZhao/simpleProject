#!/usr/bin/env bash

alias lls="tree src/main/scala/"
function wrun (){
	spark-submit --class "$1" \
		--jars /home/jason/.m2/repository/com/github/scopt/scopt_2.10/3.2.0/scopt_2.10-3.2.0.jar \
	target/scala-2.10/simple-project_2.10-1.0.jar ${@:2}
}
function run (){
	wrun $@ 2>/dev/null
}


