def get_cpu_classes():
    cpu_classes = {
        # From: https://scikit-learn.org/stable/modules/classes.html
        # sklearn.base: Base classes
        "BaseEstimator": "sklearn.base.BaseEstimator",
        "BiclusterMixin": "sklearn.base.BiclusterMixin",
        "ClassifierMixin": "sklearn.base.ClassifierMixin",
        "ClusterMixin": "sklearn.base.ClusterMixin",
        "DensityMixin": "sklearn.base.DensityMixin",
        "RegressorMixin": "sklearn.base.RegressorMixin",
        "TransformerMixin": "sklearn.base.TransformerMixin",
        "SelectorMixin": "sklearn.feature_selection.SelectorMixin",
        # sklearn.calibration: Probability Calibration
        "CalibratedClassifierCV": "sklearn.calibration.CalibratedClassifierCV",
        # sklearn.cluster: Clustering
        "AffinityPropagation": "sklearn.cluster.AffinityPropagation",
        "AgglomerativeClustering": "sklearn.cluster.AgglomerativeClustering",
        "Birch": "sklearn.cluster.Birch",
        "DBSCAN": "sklearn.cluster.DBSCAN",
        "FeatureAgglomeration": "sklearn.cluster.FeatureAgglomeration",
        "KMeans": "sklearn.cluster.KMeans",
        "BisectingKMeans": "sklearn.cluster.BisectingKMeans",
        "MiniBatchKMeans": "sklearn.cluster.MiniBatchKMeans",
        "MeanShift": "sklearn.cluster.MeanShift",
        "OPTICS": "sklearn.cluster.OPTICS",
        "SpectralClustering": "sklearn.cluster.SpectralClustering",
        "SpectralBiclustering": "sklearn.cluster.SpectralBiclustering",
        "SpectralCoclustering": "sklearn.cluster.SpectralCoclustering",
        # sklearn.compose: Composite Estimators
        "ColumnTransformer": "sklearn.compose.ColumnTransformer",
        "TransformedTargetRegressor": "sklearn.compose.TransformedTargetRegressor",
        # sklearn.covariance: Covariance Estimators
        "EmpiricalCovariance": "sklearn.covariance.EmpiricalCovariance",
        "EllipticEnvelope": "sklearn.covariance.EllipticEnvelope",
        "GraphicalLasso": "sklearn.covariance.GraphicalLasso",
        "GraphicalLassoCV": "sklearn.covariance.GraphicalLassoCV",
        "LedoitWolf": "sklearn.covariance.LedoitWolf",
        "MinCovDet": "sklearn.covariance.MinCovDet",
        "OAS": "sklearn.covariance.OAS",
        "ShrunkCovariance": "sklearn.covariance.ShrunkCovariance",
        # sklearn.cross_decomposition: Cross decomposition
        "CCA": "sklearn.cross_decomposition.CCA",
        "PLSCanonical": "sklearn.cross_decomposition.PLSCanonical",
        "PLSRegression": "sklearn.cross_decomposition.PLSRegression",
        "PLSSVD": "sklearn.cross_decomposition.PLSSVD",
        # sklearn.decomposition: Matrix Decomposition
        "DictionaryLearning": "sklearn.decomposition.DictionaryLearning",
        "FactorAnalysis": "sklearn.decomposition.FactorAnalysis",
        "FastICA": "sklearn.decomposition.FastICA",
        "IncrementalPCA": "sklearn.decomposition.IncrementalPCA",
        "KernelPCA": "sklearn.decomposition.KernelPCA",
        "LatentDirichletAllocation": "sklearn.decomposition.LatentDirichletAllocation",
        "MiniBatchDictionaryLearning": "sklearn.decomposition.MiniBatchDictionaryLearning",
        "MiniBatchSparsePCA": "sklearn.decomposition.MiniBatchSparsePCA",
        "NMF": "sklearn.decomposition.NMF",
        "MiniBatchNMF": "sklearn.decomposition.MiniBatchNMF",
        "PCA": "sklearn.decomposition.PCA",
        "SparsePCA": "sklearn.decomposition.SparsePCA",
        "SparseCoder": "sklearn.decomposition.SparseCoder",
        "TruncatedSVD": "sklearn.decomposition.TruncatedSVD",
        # sklearn.discriminant_analysis: Discriminant Analysis
        "LinearDiscriminantAnalysis": "sklearn.discriminant_analysis.LinearDiscriminantAnalysis",
        "QuadraticDiscriminantAnalysis": "sklearn.discriminant_analysis.QuadraticDiscriminantAnalysis",
        # sklearn.dummy: Dummy estimators
        "DummyClassifier": "sklearn.dummy.DummyClassifier",
        "DummyRegressor": "sklearn.dummy.DummyRegressor",
        # sklearn.ensemble: Ensemble Methods
        "AdaBoostClassifier": "sklearn.ensemble.AdaBoostClassifier",
        "AdaBoostRegressor": "sklearn.ensemble.AdaBoostRegressor",
        "BaggingClassifier": "sklearn.ensemble.BaggingClassifier",
        "BaggingRegressor": "sklearn.ensemble.BaggingRegressor",
        "ExtraTreesClassifier": "sklearn.ensemble.ExtraTreesClassifier",
        "ExtraTreesRegressor": "sklearn.ensemble.ExtraTreesRegressor",
        "GradientBoostingClassifier": "sklearn.ensemble.GradientBoostingClassifier",
        "GradientBoostingRegressor": "sklearn.ensemble.GradientBoostingRegressor",
        "IsolationForest": "sklearn.ensemble.IsolationForest",
        "RandomForestClassifier": "sklearn.ensemble.RandomForestClassifier",
        "RandomForestRegressor": "sklearn.ensemble.RandomForestRegressor",
        "RandomTreesEmbedding": "sklearn.ensemble.RandomTreesEmbedding",
        "StackingClassifier": "sklearn.ensemble.StackingClassifier",
        "StackingRegressor": "sklearn.ensemble.StackingRegressor",
        "VotingClassifier": "sklearn.ensemble.VotingClassifier",
        "VotingRegressor": "sklearn.ensemble.VotingRegressor",
        "HistGradientBoostingRegressor": "sklearn.ensemble.HistGradientBoostingRegressor",
        "HistGradientBoostingClassifier": "sklearn.ensemble.HistGradientBoostingClassifier",
        # sklearn.feature_extraction: Feature Extraction
        "DictVectorizer": "sklearn.feature_extraction.DictVectorizer",
        "FeatureHasher": "sklearn.feature_extraction.FeatureHasher",
        "PatchExtractor": "sklearn.feature_extraction.image.PatchExtractor",
        "CountVectorizer": "sklearn.feature_extraction.text.CountVectorizer",
        "HashingVectorizer": "sklearn.feature_extraction.text.HashingVectorizer",
        "TfidfTransformer": "sklearn.feature_extraction.text.TfidfTransformer",
        "TfidfVectorizer": "sklearn.feature_extraction.text.TfidfVectorizer",
        # sklearn.feature_selection: Feature Selection
        "GenericUnivariateSelect": "sklearn.feature_selection.GenericUnivariateSelect",
        "SelectPercentile": "sklearn.feature_selection.SelectPercentile",
        "SelectKBest": "sklearn.feature_selection.SelectKBest",
        "SelectFpr": "sklearn.feature_selection.SelectFpr",
        "SelectFdr": "sklearn.feature_selection.SelectFdr",
        "SelectFromModel": "sklearn.feature_selection.SelectFromModel",
        "SelectFwe": "sklearn.feature_selection.SelectFwe",
        "SequentialFeatureSelector": "sklearn.feature_selection.SequentialFeatureSelector",
        "RFE": "sklearn.feature_selection.RFE",
        "RFECV": "sklearn.feature_selection.RFECV",
        "VarianceThreshold": "sklearn.feature_selection.VarianceThreshold",
        # sklearn.gaussian_process: Gaussian Processes
        "GaussianProcessClassifier": "sklearn.gaussian_process.GaussianProcessClassifier",
        "GaussianProcessRegressor": "sklearn.gaussian_process.GaussianProcessRegressor",
        "CompoundKernel": "sklearn.gaussian_process.kernels.CompoundKernel",
        "ConstantKernel": "sklearn.gaussian_process.kernels.ConstantKernel",
        "DotProduct": "sklearn.gaussian_process.kernels.DotProduct",
        "ExpSineSquared": "sklearn.gaussian_process.kernels.ExpSineSquared",
        "Exponentiation": "sklearn.gaussian_process.kernels.Exponentiation",
        "Hyperparameter": "sklearn.gaussian_process.kernels.Hyperparameter",
        "Kernel": "sklearn.gaussian_process.kernels.Kernel",
        "Matern": "sklearn.gaussian_process.kernels.Matern",
        "PairwiseKernel": "sklearn.gaussian_process.kernels.PairwiseKernel",
        "Product": "sklearn.gaussian_process.kernels.Product",
        "RBF": "sklearn.gaussian_process.kernels.RBF",
        "RationalQuadratic": "sklearn.gaussian_process.kernels.RationalQuadratic",
        "Sum": "sklearn.gaussian_process.kernels.Sum",
        "WhiteKernel": "sklearn.gaussian_process.kernels.WhiteKernel",
        # sklearn.impute: Impute
        "SimpleImputer": "sklearn.impute.SimpleImputer",
        "IterativeImputer": "sklearn.impute.IterativeImputer",
        "MissingIndicator": "sklearn.impute.MissingIndicator",
        "KNNImputer": "sklearn.impute.KNNImputer",
        # sklearn.isotonic: Isotonic regression
        "IsotonicRegression": "sklearn.isotonic.IsotonicRegression",
        # sklearn.kernel_approximation: Kernel Approximation
        "AdditiveChi2Sampler": "sklearn.kernel_approximation.AdditiveChi2Sampler",
        "Nystroem": "sklearn.kernel_approximation.Nystroem",
        "PolynomialCountSketch": "sklearn.kernel_approximation.PolynomialCountSketch",
        "RBFSampler": "sklearn.kernel_approximation.RBFSampler",
        "SkewedChi2Sampler": "sklearn.kernel_approximation.SkewedChi2Sampler",
        # sklearn.kernel_ridge: Kernel Ridge Regression
        "KernelRidge": "sklearn.kernel_ridge.KernelRidge",
        # sklearn.linear_model: Linear Models
        "LogisticRegression": "sklearn.linear_model.LogisticRegression",
        "LogisticRegressionCV": "sklearn.linear_model.LogisticRegressionCV",
        "PassiveAggressiveClassifier": "sklearn.linear_model.PassiveAggressiveClassifier",
        "Perceptron": "sklearn.linear_model.Perceptron",
        "RidgeClassifier": "sklearn.linear_model.RidgeClassifier",
        "RidgeClassifierCV": "sklearn.linear_model.RidgeClassifierCV",
        "SGDClassifier": "sklearn.linear_model.SGDClassifier",
        "SGDOneClassSVM": "sklearn.linear_model.SGDOneClassSVM",
        "LinearRegression": "sklearn.linear_model.LinearRegression",
        "Ridge": "sklearn.linear_model.Ridge",
        "RidgeCV": "sklearn.linear_model.RidgeCV",
        "SGDRegressor": "sklearn.linear_model.SGDRegressor",
        "ElasticNet": "sklearn.linear_model.ElasticNet",
        "ElasticNetCV": "sklearn.linear_model.ElasticNetCV",
        "Lars": "sklearn.linear_model.Lars",
        "LarsCV": "sklearn.linear_model.LarsCV",
        "Lasso": "sklearn.linear_model.Lasso",
        "LassoCV": "sklearn.linear_model.LassoCV",
        "LassoLars": "sklearn.linear_model.LassoLars",
        "LassoLarsCV": "sklearn.linear_model.LassoLarsCV",
        "LassoLarsIC": "sklearn.linear_model.LassoLarsIC",
        "OrthogonalMatchingPursuit": "sklearn.linear_model.OrthogonalMatchingPursuit",
        "OrthogonalMatchingPursuitCV": "sklearn.linear_model.OrthogonalMatchingPursuitCV",
        "ARDRegression": "sklearn.linear_model.ARDRegression",
        "BayesianRidge": "sklearn.linear_model.BayesianRidge",
        "MultiTaskElasticNet": "sklearn.linear_model.MultiTaskElasticNet",
        "MultiTaskElasticNetCV": "sklearn.linear_model.MultiTaskElasticNetCV",
        "MultiTaskLasso": "sklearn.linear_model.MultiTaskLasso",
        "MultiTaskLassoCV": "sklearn.linear_model.MultiTaskLassoCV",
        "HuberRegressor": "sklearn.linear_model.HuberRegressor",
        "QuantileRegressor": "sklearn.linear_model.QuantileRegressor",
        "RANSACRegressor": "sklearn.linear_model.RANSACRegressor",
        "TheilSenRegressor": "sklearn.linear_model.TheilSenRegressor",
        "PoissonRegressor": "sklearn.linear_model.PoissonRegressor",
        "TweedieRegressor": "sklearn.linear_model.TweedieRegressor",
        "GammaRegressor": "sklearn.linear_model.GammaRegressor",
        "PassiveAggressiveRegressor": "sklearn.linear_model.PassiveAggressiveRegressor",
        # sklearn.manifold: Manifold Learning
        "Isomap": "sklearn.manifold.Isomap",
        "LocallyLinearEmbedding": "sklearn.manifold.LocallyLinearEmbedding",
        "MDS": "sklearn.manifold.MDS",
        "SpectralEmbedding": "sklearn.manifold.SpectralEmbedding",
        "TSNE": "sklearn.manifold.TSNE",
        # sklearn.mixture: Gaussian Mixture Models
        "BayesianGaussianMixture": "sklearn.mixture.BayesianGaussianMixture",
        "GaussianMixture": "sklearn.mixture.GaussianMixture",
        # sklearn.model_selection: Model Selection
        "GroupKFold": "sklearn.model_selection.GroupKFold",
        "GroupShuffleSplit": "sklearn.model_selection.GroupShuffleSplit",
        "KFold": "sklearn.model_selection.KFold",
        "LeaveOneGroupOut": "sklearn.model_selection.LeaveOneGroupOut",
        "LeavePGroupsOut": "sklearn.model_selection.LeavePGroupsOut",
        "LeaveOneOut": "sklearn.model_selection.LeaveOneOut",
        "LeavePOut": "sklearn.model_selection.LeavePOut",
        "PredefinedSplit": "sklearn.model_selection.PredefinedSplit",
        "RepeatedKFold": "sklearn.model_selection.RepeatedKFold",
        "RepeatedStratifiedKFold": "sklearn.model_selection.RepeatedStratifiedKFold",
        "ShuffleSplit": "sklearn.model_selection.ShuffleSplit",
        "StratifiedKFold": "sklearn.model_selection.StratifiedKFold",
        "StratifiedShuffleSplit": "sklearn.model_selection.StratifiedShuffleSplit",
        "StratifiedGroupKFold": "sklearn.model_selection.StratifiedGroupKFold",
        "TimeSeriesSplit": "sklearn.model_selection.TimeSeriesSplit",
        "GridSearchCV": "sklearn.model_selection.GridSearchCV",
        "HalvingGridSearchCV": "sklearn.model_selection.HalvingGridSearchCV",
        "ParameterGrid": "sklearn.model_selection.ParameterGrid",
        "ParameterSampler": "sklearn.model_selection.ParameterSampler",
        "RandomizedSearchCV": "sklearn.model_selection.RandomizedSearchCV",
        "HalvingRandomSearchCV": "sklearn.model_selection.HalvingRandomSearchCV",
        # sklearn.multiclass: Multiclass classification
        "OneVsRestClassifier": "sklearn.multiclass.OneVsRestClassifier",
        "OneVsOneClassifier": "sklearn.multiclass.OneVsOneClassifier",
        "OutputCodeClassifier": "sklearn.multiclass.OutputCodeClassifier",
        # sklearn.multioutput: Multioutput regression and classification
        "ClassifierChain": "sklearn.multioutput.ClassifierChain",
        "MultiOutputRegressor": "sklearn.multioutput.MultiOutputRegressor",
        "MultiOutputClassifier": "sklearn.multioutput.MultiOutputClassifier",
        "RegressorChain": "sklearn.multioutput.RegressorChain",
        # sklearn.naive_bayes: Naive Bayes
        "BernoulliNB": "sklearn.naive_bayes.BernoulliNB",
        "CategoricalNB": "sklearn.naive_bayes.CategoricalNB",
        "ComplementNB": "sklearn.naive_bayes.ComplementNB",
        "GaussianNB": "sklearn.naive_bayes.GaussianNB",
        "MultinomialNB": "sklearn.naive_bayes.MultinomialNB",
        # sklearn.neighbors: Nearest Neighbors
        "BallTree": "sklearn.neighbors.BallTree",
        "KDTree": "sklearn.neighbors.KDTree",
        "KernelDensity": "sklearn.neighbors.KernelDensity",
        "KNeighborsClassifier": "sklearn.neighbors.KNeighborsClassifier",
        "KNeighborsRegressor": "sklearn.neighbors.KNeighborsRegressor",
        "KNeighborsTransformer": "sklearn.neighbors.KNeighborsTransformer",
        "LocalOutlierFactor": "sklearn.neighbors.LocalOutlierFactor",
        "RadiusNeighborsClassifier": "sklearn.neighbors.RadiusNeighborsClassifier",
        "RadiusNeighborsRegressor": "sklearn.neighbors.RadiusNeighborsRegressor",
        "RadiusNeighborsTransformer": "sklearn.neighbors.RadiusNeighborsTransformer",
        "NearestCentroid": "sklearn.neighbors.NearestCentroid",
        "NearestNeighbors": "sklearn.neighbors.NearestNeighbors",
        "NeighborhoodComponentsAnalysis": "sklearn.neighbors.NeighborhoodComponentsAnalysis",
        # sklearn.neural_network: Neural network models
        "BernoulliRBM": "sklearn.neural_network.BernoulliRBM",
        "MLPClassifier": "sklearn.neural_network.MLPClassifier",
        "MLPRegressor": "sklearn.neural_network.MLPRegressor",
        # sklearn.pipeline: Pipeline
        "FeatureUnion": "sklearn.pipeline.FeatureUnion",
        "Pipeline": "sklearn.pipeline.Pipeline",
        # sklearn.preprocessing: Preprocessing and Normalization
        "Binarizer": "sklearn.preprocessing.Binarizer",
        "FunctionTransformer": "sklearn.preprocessing.FunctionTransformer",
        "KBinsDiscretizer": "sklearn.preprocessing.KBinsDiscretizer",
        "KernelCenterer": "sklearn.preprocessing.KernelCenterer",
        "LabelBinarizer": "sklearn.preprocessing.LabelBinarizer",
        "LabelEncoder": "sklearn.preprocessing.LabelEncoder",
        "MultiLabelBinarizer": "sklearn.preprocessing.MultiLabelBinarizer",
        "MaxAbsScaler": "sklearn.preprocessing.MaxAbsScaler",
        "MinMaxScaler": "sklearn.preprocessing.MinMaxScaler",
        "Normalizer": "sklearn.preprocessing.Normalizer",
        "OneHotEncoder": "sklearn.preprocessing.OneHotEncoder",
        "OrdinalEncoder": "sklearn.preprocessing.OrdinalEncoder",
        "PolynomialFeatures": "sklearn.preprocessing.PolynomialFeatures",
        "PowerTransformer": "sklearn.preprocessing.PowerTransformer",
        "QuantileTransformer": "sklearn.preprocessing.QuantileTransformer",
        "RobustScaler": "sklearn.preprocessing.RobustScaler",
        "SplineTransformer": "sklearn.preprocessing.SplineTransformer",
        "StandardScaler": "sklearn.preprocessing.StandardScaler",
        # sklearn.random_projection: Random projection
        "GaussianRandomProjection": "sklearn.random_projection.GaussianRandomProjection",
        "SparseRandomProjection": "sklearn.random_projection.SparseRandomProjection",
        # sklearn.semi_supervised: Semi-Supervised Learning
        "LabelPropagation": "sklearn.semi_supervised.LabelPropagation",
        "LabelSpreading": "sklearn.semi_supervised.LabelSpreading",
        "SelfTrainingClassifier": "sklearn.semi_supervised.SelfTrainingClassifier",
        # sklearn.svm: Support Vector Machines
        "LinearSVC": "sklearn.svm.LinearSVC",
        "LinearSVR": "sklearn.svm.LinearSVR",
        "NuSVC": "sklearn.svm.NuSVC",
        "NuSVR": "sklearn.svm.NuSVR",
        "OneClassSVM": "sklearn.svm.OneClassSVM",
        "SVC": "sklearn.svm.SVC",
        "SVR": "sklearn.svm.SVR",
        # sklearn.tree: Decision Trees
        "DecisionTreeClassifier": "sklearn.tree.DecisionTreeClassifier",
        "DecisionTreeRegressor": "sklearn.tree.DecisionTreeRegressor",
        "ExtraTreeClassifier": "sklearn.tree.ExtraTreeClassifier",
        "ExtraTreeRegressor": "sklearn.tree.ExtraTreeRegressor",
        # Other
        "LGBMClassifier": "lightgbm.LGBMClassifier",
        "XGBRegressor": "xgboost.XGBRegressor",
        "DaskXGBRegressor": "xgboost.dask.DaskXGBRegressor",
        "XGBClassifier": "xgboost.XGBClassifier",
        "DaskXGBClassifier": "xgboost.dask.DaskXGBClassifier",
    }
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
        # ImportError: dask-glm >= 0.2.1.dev was not found, please install it to use multi-GPU logistic regression.
        # "LogisticRegression": "cuml.dask.extended.linear_model.logistic_regression.LogisticRegression",
        "LogisticRegression": "cuml.linear_model.LogisticRegression",
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
        # XGBoost
        "LGBMClassifier": "lightgbm.LGBMClassifier",  # not compatible on GPU
        "XGBRegressor": "xgboost.XGBRegressor",
        "DaskXGBRegressor": "xgboost.dask.DaskXGBRegressor",
        "XGBClassifier": "xgboost.XGBClassifier",  # not compatible on GPU
        "DaskXGBClassifier": "xgboost.dask.DaskXGBClassifier",  # not compatible on GPU
    }
    return gpu_classes
