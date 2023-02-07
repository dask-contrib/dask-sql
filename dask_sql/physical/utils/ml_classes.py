def get_cpu_classes():
    try:
        from sklearn.utils import all_estimators

        cpu_classes = {
            k: v.__module__ + "." + v.__qualname__ for k, v in all_estimators()
        }
    except ImportError:
        cpu_classes = {}

    cpu_classes = add_boosting_classes(cpu_classes)

    return cpu_classes


def get_gpu_classes():
    gpu_classes = {
        # cuml.dask
        "DBSCAN": "cuml.dask.cluster.dbscan.DBSCAN",
        "KMeans": "cuml.dask.cluster.kmeans.KMeans",
        "PCA": "cuml.dask.decomposition.pca.PCA",
        "TruncatedSVD": "cuml.dask.decomposition.tsvd.TruncatedSVD",
        "RandomForestClassifier": "cuml.dask.ensemble.randomforestclassifier.RandomForestClassifier",
        "RandomForestRegressor": "cuml.dask.ensemble.randomforestregressor.RandomForestRegressor",
        "LogisticRegression": "cuml.dask.extended.linear_model.logistic_regression.LogisticRegression",
        "TfidfTransformer": "cuml.dask.feature_extraction.text.tfidf_transformer.TfidfTransformer",
        "LinearRegression": "cuml.dask.linear_model.linear_regression.LinearRegression",
        "Ridge": "cuml.dask.linear_model.ridge.Ridge",
        "Lasso": "cuml.dask.linear_model.lasso.Lasso",
        "ElasticNet": "cuml.dask.linear_model.elastic_net.ElasticNet",
        "UMAP": "cuml.dask.manifold.umap.UMAP",
        "MultinomialNB": "cuml.dask.naive_bayes.naive_bayes.MultinomialNB",
        "NearestNeighbors": "cuml.dask.neighbors.nearest_neighbors.NearestNeighbors",
        "KNeighborsClassifier": "cuml.dask.neighbors.kneighbors_classifier.KNeighborsClassifier",
        "KNeighborsRegressor": "cuml.dask.neighbors.kneighbors_regressor.KNeighborsRegressor",
        "LabelBinarizer": "cuml.dask.preprocessing.label.LabelBinarizer",
        "OneHotEncoder": "cuml.dask.preprocessing.encoders.OneHotEncoder",
        "LabelEncoder": "cuml.dask.preprocessing.LabelEncoder.LabelEncoder",
        "CD": "cuml.dask.solvers.cd.CD",
        # cuml
        "Base": "cuml.internals.base.Base",
        "Handle": "cuml.common.handle.Handle",
        "AgglomerativeClustering": "cuml.cluster.agglomerative.AgglomerativeClustering",
        "HDBSCAN": "cuml.cluster.hdbscan.HDBSCAN",
        "IncrementalPCA": "cuml.decomposition.incremental_pca.IncrementalPCA",
        "ForestInference": "cuml.fil.fil.ForestInference",
        "KernelRidge": "cuml.kernel_ridge.kernel_ridge.KernelRidge",
        "MBSGDClassifier": "cuml.linear_model.mbsgd_classifier.MBSGDClassifier",
        "MBSGDRegressor": "cuml.linear_model.mbsgd_regressor.MBSGDRegressor",
        "TSNE": "cuml.manifold.t_sne.TSNE",
        "KernelDensity": "cuml.neighbors.kernel_density.KernelDensity",
        "GaussianRandomProjection": "cuml.random_projection.random_projection.GaussianRandomProjection",
        "SparseRandomProjection": "cuml.random_projection.random_projection.SparseRandomProjection",
        "SGD": "cuml.solvers.sgd.SGD",
        "QN": "cuml.solvers.qn.QN",
        "SVC": "cuml.svm.SVC",
        "SVR": "cuml.svm.SVR",
        "LinearSVC": "cuml.svm.LinearSVC",
        "LinearSVR": "cuml.svm.LinearSVR",
        "ARIMA": "cuml.tsa.arima.ARIMA",
        "AutoARIMA": "cuml.tsa.auto_arima.AutoARIMA",
        "ExponentialSmoothing": "cuml.tsa.holtwinters.ExponentialSmoothing",
        # sklearn
        "Binarizer": "cuml.preprocessing.Binarizer",
        "KernelCenterer": "cuml.preprocessing.KernelCenterer",
        "MinMaxScaler": "cuml.preprocessing.MinMaxScaler",
        "MaxAbsScaler": "cuml.preprocessing.MaxAbsScaler",
        "Normalizer": "cuml.preprocessing.Normalizer",
        "PolynomialFeatures": "cuml.preprocessing.PolynomialFeatures",
        "PowerTransformer": "cuml.preprocessing.PowerTransformer",
        "QuantileTransformer": "cuml.preprocessing.QuantileTransformer",
        "RobustScaler": "cuml.preprocessing.RobustScaler",
        "StandardScaler": "cuml.preprocessing.StandardScaler",
        "SimpleImputer": "cuml.preprocessing.SimpleImputer",
        "MissingIndicator": "cuml.preprocessing.MissingIndicator",
        "KBinsDiscretizer": "cuml.preprocessing.KBinsDiscretizer",
        "FunctionTransformer": "cuml.preprocessing.FunctionTransformer",
        "ColumnTransformer": "cuml.compose.ColumnTransformer",
        "GridSearchCV": "sklearn.model_selection.GridSearchCV",
        "Pipeline": "sklearn.pipeline.Pipeline",
        # Other
        "UniversalBase": "cuml.internals.base.UniversalBase",
        "Lars": "cuml.experimental.linear_model.lars.Lars",
        "TfidfVectorizer": "cuml.feature_extraction._tfidf_vectorizer.TfidfVectorizer",
        "CountVectorizer": "cuml.feature_extraction._vectorizers.CountVectorizer",
        "HashingVectorizer": "cuml.feature_extraction._vectorizers.HashingVectorizer",
        "StratifiedKFold": "cuml.model_selection._split.StratifiedKFold",
        "OneVsOneClassifier": "cuml.multiclass.multiclass.OneVsOneClassifier",
        "OneVsRestClassifier": "cuml.multiclass.multiclass.OneVsRestClassifier",
        "MulticlassClassifier": "cuml.multiclass.multiclass.MulticlassClassifier",
        "BernoulliNB": "cuml.naive_bayes.naive_bayes.BernoulliNB",
        "GaussianNB": "cuml.naive_bayes.naive_bayes.GaussianNB",
        "ComplementNB": "cuml.naive_bayes.naive_bayes.ComplementNB",
        "CategoricalNB": "cuml.naive_bayes.naive_bayes.CategoricalNB",
        "TargetEncoder": "cuml.preprocessing.TargetEncoder",
        "PorterStemmer": "cuml.preprocessing.text.stem.porter_stemmer.PorterStemmer",
    }

    gpu_classes = add_boosting_classes(gpu_classes)

    return gpu_classes


def add_boosting_classes(my_classes):
    my_classes["LGBMModel"] = "lightgbm.LGBMModel"
    my_classes["LGBMClassifier"] = "lightgbm.LGBMClassifier"
    my_classes["LGBMRegressor"] = "lightgbm.LGBMRegressor"
    my_classes["LGBMRanker"] = "lightgbm.LGBMRanker"
    my_classes["XGBRegressor"] = "xgboost.XGBRegressor"
    my_classes["XGBClassifier"] = "xgboost.XGBClassifier"
    my_classes["XGBRanker"] = "xgboost.XGBRanker"
    my_classes["XGBRFRegressor"] = "xgboost.XGBRFRegressor"
    my_classes["XGBRFClassifier"] = "xgboost.XGBRFClassifier"
    my_classes["DaskXGBClassifier"] = "xgboost.dask.DaskXGBClassifier"
    my_classes["DaskXGBRegressor"] = "xgboost.dask.DaskXGBRegressor"
    my_classes["DaskXGBRanker"] = "xgboost.dask.DaskXGBRanker"
    my_classes["DaskXGBRFRegressor"] = "xgboost.dask.DaskXGBRFRegressor"
    my_classes["DaskXGBRFClassifier"] = "xgboost.dask.DaskXGBRFClassifier"

    return my_classes
