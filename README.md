# dask-sql-website 

This repo contains everything needed for the dask-sql website 

To start developing, run

	conda create -n dask-sql-website nodejs
	conda activate dask-sql-website
	npm install

And for starting the server

	npm run dev	

## Deployment

Deployment goes via github actions to gh-pages. You can also do it manually:

	npm run export
	npm run deploy
