.PHONY: build clean run

# Definim classpath-ul relativ la folderul curent (src)
CP = .:libs/jackson-core-2.17.0.jar:libs/jackson-annotations-2.17.0.jar:libs/jackson-databind-2.17.0.jar

build:
	javac -cp $(CP) *.java

run:
	java -cp $(CP) Tema1 $(ARGS)

clean:
	rm -f *.class