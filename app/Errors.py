class ErrorPumoriLogin(Exception):
    def __init__(self, *args: object) -> None:
        message = "Failed to login Pumori"
        super().__init__(message)


class WeightageParamsError(Exception):
    def __init__(self, *args) -> None:
        if args:
            message = f"Following parameter is not provided {args}."
        else:
            message = "Parameter input is wrong."
        super().__init__(message)


class WeightageProcessError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = (
                f"Weightage retrived process failed due to following arguments: {args}."
            )
        else:
            message = "Weightage retrived process failed."
        super().__init__(message)


class FileReadError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = f"Failed to read excel file from source due to following arguments: {args}."
        else:
            message = "Failed to read excel file from source folder."
        super().__init__(message)


class DataNotFoundError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = f"Cannot found data related: {args}."
        else:
            message = "Cannot found any data in Dataframe."
        super().__init__(message)


class MultipleDataError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = f"More than one data found for {args}."
        else:
            message = "More than one data found."
        super().__init__(message)


class FlieAlreadyReadError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = f"{args}  file is read by bot already."
        else:
            message = "File is read by bot already."
        super().__init__(message)


class NatureOfAccountError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = f"Unknown nature of account: \n {args}"
        else:
            message = "Unknown nature of account"
        super().__init__(message)


class SearchTypeError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = f"Search type generation error \n {args}"
        else:
            message = "Search type generation error"
        super().__init__(message)


class NoUniqueIDError(Exception):
    def __init__(self, *args: object) -> None:
        if args:
            message = f"Search type generation error \n {args}"
        else:
            message = "Search type generation error"
        super().__init__(message)

