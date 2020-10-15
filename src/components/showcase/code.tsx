import { FC, ReactNode } from "react";

interface CodeLineProps {
    children: ReactNode
    indent: number
}

export const CodeLine: FC<CodeLineProps> = (props) => {
    const indentationText: string = " ".repeat(props.indent);
    return <p className="whitespace-no-wrap"><span style={{ whiteSpace: "pre" }}>{indentationText}</span>{props.children}</p>
};


interface CommentProps {
    children: ReactNode
}

export const Comment: FC<CommentProps> = (props) => (
    <span className="text-gray-500">{props.children}</span>
)

interface KeywordProps {
    children: ReactNode
}

export const Keyword: FC<KeywordProps> = (props) => (
    <span className="text-purple-500 font-bold">{props.children}</span>
)

interface StringProps {
    children: ReactNode
}

export const String: FC<StringProps> = (props) => (
    <span className="text-indigo-400">{props.children}</span>
)
