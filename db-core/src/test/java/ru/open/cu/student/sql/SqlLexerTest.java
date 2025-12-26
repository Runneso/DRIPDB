package ru.open.cu.student.sql;

import org.junit.jupiter.api.Test;
import ru.open.cu.student.sql.lexer.SqlLexer;
import ru.open.cu.student.sql.lexer.SqlSyntaxException;
import ru.open.cu.student.sql.lexer.Token;
import ru.open.cu.student.sql.lexer.TokenType;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class SqlLexerTest {

    @Test
    void tokenizes_select_where() {
        SqlLexer lexer = new SqlLexer();
        List<Token> tokens = lexer.tokenize("SELECT name, age FROM users WHERE age >= 18;");
        List<TokenType> types = tokens.stream().map(Token::getType).collect(Collectors.toList());

        assertEquals(
                List.of(
                        TokenType.SELECT,
                        TokenType.IDENT,
                        TokenType.COMMA,
                        TokenType.IDENT,
                        TokenType.FROM,
                        TokenType.IDENT,
                        TokenType.WHERE,
                        TokenType.IDENT,
                        TokenType.GE,
                        TokenType.NUMBER,
                        TokenType.SEMICOLON,
                        TokenType.EOF
                ),
                types
        );
    }

    @Test
    void tokenizes_string_with_escaped_quote() {
        SqlLexer lexer = new SqlLexer();
        List<Token> tokens = lexer.tokenize("INSERT INTO t VALUES ('it''s');");
        Token stringTok = tokens.stream().filter(t -> t.getType() == TokenType.STRING).findFirst().orElseThrow();
        assertEquals("it's", stringTok.getText());
    }

    @Test
    void unterminated_string_throws_with_position() {
        SqlLexer lexer = new SqlLexer();
        SqlSyntaxException ex = assertThrows(SqlSyntaxException.class, () -> lexer.tokenize("SELECT 'abc"));
        assertTrue(ex.getMessage().toLowerCase().contains("unterminated"));
        assertTrue(ex.getOffset() >= 0);
        assertTrue(ex.getLine() >= 1);
        assertTrue(ex.getColumn() >= 1);
    }
}


